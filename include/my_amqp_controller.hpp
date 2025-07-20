#pragma once
#include <iostream>
#include <memory>

#include <atomic>
#include <amqpcpp/address.h>
#include <amqpcpp/libevent.h>
#include "channel_config.hpp"

#include "my_amqp_channel.hpp"
#include "rx_client_wrapper.hpp"
#include "tx_client_wrapper.hpp"

namespace rmq
{

class MyLibEventHandler final : public AMQP::LibEventHandler
{
public:
	explicit MyLibEventHandler(struct event_base *evbase, std::vector<event*> &events) : LibEventHandler(evbase), events_(events)
	{

	}

	~MyLibEventHandler() = default;

	void onReady(AMQP::TcpConnection *connection) override
	{
		// std::cout << "onReady - connection is now ready" << std::endl;
		is_ready_ = true;
	}

	void onError(AMQP::TcpConnection *connection, const char *message) override
	{
		std::cout << "onError - connection error: " << message << std::endl;
		is_error_ = true;
		for (const auto& event : events_)
		{
			event_del(event);
			event_free(event);
		}
		events_.clear();
		connection->close();
		// TODO Need to be able to signal back to the AmqpController that we need to cleanup other events
	}

	bool isError() const
	{
		return is_error_.load();
	}

	bool isReady() const
	{
		return is_ready_.load();
	}

private:
	std::vector<event*> &events_;
	std::atomic<bool> is_ready_ {false};
	std::atomic<bool> is_error_ {false};

};


using ChannelListenerPtr = std::shared_ptr<ChannelListener>;
using ChannelHandlerTxPtr = std::unique_ptr<MyAmqpTxChannel>;
using ChannelHandlerRxPtr = std::unique_ptr<MyAmqpRxChannel>;
template <typename MessageType, typename ChannelTypePtr>
class MyAmqpChannelInfo
{
public:
	explicit MyAmqpChannelInfo(ChannelTypePtr channel
		, const ChannelListenerPtr &listener
		, const std::shared_ptr<MessageType>& queue
		, const ChannelConfig& config)
		: channel_(std::move(channel))
		, queue_(queue)
		, config_(config)
		, listener_(listener)
	{}

	void resetChannel()
	{
		channel_.reset();
	}

	void setChannel(ChannelTypePtr channel)
	{
		channel_.reset();
		channel_ = std::move(channel);
	}

	[[nodiscard]] std::shared_ptr<MessageType> queue() const
	{
		return queue_;
	}

	[[nodiscard]] ChannelConfig config() const
	{
		return config_;
	}

	[[nodiscard]] ChannelListenerPtr listener()
	{
		return listener_;
	}

	void setListener(const ChannelListenerPtr& listener)
	{
		listener_ = listener;
	}

protected:
	ChannelTypePtr channel_;
	std::shared_ptr<MessageType> queue_;
	ChannelConfig config_;
	std::shared_ptr<ChannelListener> listener_;
};


class MyAmqpTxChannelInfo : public MyAmqpChannelInfo<MyTxDataQueue, ChannelHandlerTxPtr>
{
public:
	explicit MyAmqpTxChannelInfo(ChannelHandlerTxPtr channel
	                             , const ChannelListenerPtr &listener
	                             , const std::shared_ptr<MyTxDataQueue> &queue
	                             , const ChannelConfig &config) :
		MyAmqpChannelInfo(std::move(channel), listener, queue, config)
	{
	}
};

class MyAmqpRxChannelInfo : public MyAmqpChannelInfo<MyRxDataQueue, ChannelHandlerRxPtr>
{
public:
	explicit MyAmqpRxChannelInfo(ChannelHandlerRxPtr channel
	                             , const ChannelListenerPtr &listener
	                             , const std::shared_ptr<MyRxDataQueue> &queue
	                             , const ChannelConfig &config) :
		MyAmqpChannelInfo(std::move(channel), listener, queue, config)
	{
	}

	void acknowledge(const IMessageAck& ack) const
	{
		channel_->acknowledge(ack);
	}
};

class MyAmqpController {
public:
	using TxChannelHandlerPtr = std::shared_ptr<MyAmqpTxChannel>;
	explicit MyAmqpController(const std::string& address) :
		address_(address)
		, evbase_(nullptr)
	{

	}

	~MyAmqpController()
	{
		// std::cout << "MyAmqpController::~MyAmqpController()" << std::endl;
		if (maintain_connection.load())
		{
			maintain_connection.store(false);
			triggerClose();
		}

		// Close the connection - this should also stop the event handling loop when complete
		// TODO Use a notification pipe to close this
		// connection_->close();

		maintain_connection_thread.join(); // TODO Probably don't need the flag above anymore
		// std::cout << "MyAmqpController::~MyAmqpController() - done" << std::endl;
	}

	TxClientWrapper createTransmitChannel(const ChannelConfig& config, ChannelListenerPtr listener=std::make_shared<ChannelListener>())
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (!listener)
		{
			throw std::runtime_error("Cannot accept nullptr listener");
		}

		auto channel_name = config.exchange_name;
		auto amqp_channel = std::make_unique<AMQP::TcpChannel>(connection_.get());
		auto queue = std::make_shared<MyTxDataQueue>(1000, QueueOverflowPolicy::WAIT); // possibly pass this in as a parameter?
		auto tx_channel = std::make_unique<MyAmqpTxChannel>(std::move(amqp_channel)
			, config
			, [&config, this](const std::string &error_message) { onChannelError(config.queue_name, error_message.c_str()); }
			, queue
			, listener);
		transmit_functions_.emplace_back(std::bind(&MyAmqpTxChannel::sendData, tx_channel.get(), std::placeholders::_1));
		tx_channel_wrappers_.emplace(channel_name, MyAmqpTxChannelInfo(std::move(tx_channel), listener, queue, config));

		return TxClientWrapper(channel_name, listener, queue);
	}

	RxClientWrapper createReceiveChannel(const ChannelConfig& config, ChannelListenerPtr listener=std::make_shared<ChannelListener>())
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (!listener)
		{
			throw std::runtime_error("Cannot accept nullptr listener");
		}

		auto channel_name = config.queue_name;
		auto amqp_channel = std::make_unique<AMQP::TcpChannel>(connection_.get());
		auto queue = std::make_shared<MyRxDataQueue>(1000, QueueOverflowPolicy::WAIT); // possibly pass this in as a parameter?
		auto rx_channel = std::make_unique<MyAmqpRxChannel>(std::move(amqp_channel)
			, config
			, [channel_name, this](const std::string &error_message) { onChannelError(channel_name, error_message.c_str()); }
			, queue
			, listener);
		auto ack_fn = std::bind(&MyAmqpRxChannel::acknowledge, rx_channel.get(), std::placeholders::_1);
		rx_channel_wrappers_.emplace(channel_name, MyAmqpRxChannelInfo(std::move(rx_channel), listener, queue, config));

		// Need a way to access bits in the future
		return RxClientWrapper(channel_name, listener, queue, ack_fn);
	}
	
	bool isConnectionReady() const
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (!handler_) { return false; }
		return handler_->isReady();
	}

	bool isChannelReady(const std::string& channel_name)
	{
		std::lock_guard<std::mutex> lock(mutex_);
		const auto it = tx_channel_wrappers_.find(channel_name);
		if (it == tx_channel_wrappers_.end())
		{
			return false;
		}
		return it->second.listener()->isActive();
	}

	bool isConnectionError() const
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return handler_->isError();
	}

	void start()
	{
		std::lock_guard<std::mutex> lock(mutex_);
		// Check we aren't already running!
		if (!maintain_connection_thread.joinable())
		{
			maintain_connection_thread = std::thread(&MyAmqpController::run, this);
		}
	}


	void run()
	{
		while (maintain_connection.load())
		{
			establishAmqpConnection_();

			// This will only return once all registered event sources (controller and channels) have finished
			event_base_dispatch(evbase_);

			cleanUpAmqpConnection_();

			++num_reconnections;
			LOG_INFO("Finished reconnecting to " << address_);

#if 0
			if (maintain_connection.load())
			{
				// Stop the transmit thread here - important
				cmd_transmit_active.store(false);
				while (res_transmit_active.load())
				{
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}

				transmit_functions_.clear();

				// Stop the channels - get rid of the old channel info that is now no longer working and will be connected to an obsolete connection
				for (auto& tx_channel_wrapper : tx_channel_wrappers_)
				{
					tx_channel_wrapper.second.resetChannel();
				}
				for (auto& rx_channel_wrapper : rx_channel_wrappers_)
				{
					rx_channel_wrapper.second.resetChannel();
				}

				// Set up a new connection
				std::cout << "Reconnecting to " << address_ << std::endl;
				handler_ = std::make_unique<MyLibEventHandler>(evbase_);
				connection_ = std::make_unique<AMQP::TcpConnection>(handler_.get(), AMQP::Address(address_));

				// Reset the transmitters
				for (auto& channel_wrapper : tx_channel_wrappers_)
				{
					// Create a new channel and connect it to the new connection
					auto amqp_channel = std::make_unique<AMQP::TcpChannel>(connection_.get());
					auto tx_channel = std::make_unique<MyAmqpTxChannel>(
						std::move(amqp_channel)
						, channel_wrapper.second.config()
						, [&channel_wrapper, this](const std::string &error_message) { onChannelError(channel_wrapper.first, error_message.c_str()); }
						, channel_wrapper.second.queue()
						, channel_wrapper.second.listener()
						, channel_wrapper.second.listener()->getNumberOfTransmittedMessages()
						);
					transmit_functions_.emplace_back(std::bind(&MyAmqpTxChannel::sendData, tx_channel.get(), std::placeholders::_1));
					channel_wrapper.second.setChannel(std::move(tx_channel));
				}
				cmd_transmit_active.store(true);

				// Reset the receivers TODO


			}
#endif
		}
	}

	int getNumReconnections() const
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return num_reconnections.load();
	}

	void onChannelError(const std::string& channel_name, const char *message) const
	{
		std::lock_guard<std::mutex> lock(mutex_);
		// Force a reconnection and restart of all the channels - from experience just restarting the affected channel doesn't work
		LOG_ERROR(message);
		connection_->close();
	}

	// TODO do we still need this at all?
	MyTxDataQueuePtr getTxQueue(const std::string& channel_name)
	{
		std::lock_guard<std::mutex> lock(mutex_);
		const auto it = tx_channel_wrappers_.find(channel_name);
		if (it == tx_channel_wrappers_.end())
		{
			return nullptr;
		}
		return it->second.queue();
	}

	// TODO do we still need this at all?
	MyRxDataQueuePtr getRxQueue(const std::string& channel_name)
	{
		std::lock_guard<std::mutex> lock(mutex_);
		const auto it = rx_channel_wrappers_.find(channel_name);
		if (it == rx_channel_wrappers_.end())
		{
			return nullptr;
		}
		return it->second.queue();
	}

	// TODO do we still need this at all?
	std::function<void(const IMessageAck& ack)> getAckFunction(const std::string& channel_name)
	{
		std::lock_guard<std::mutex> lock(mutex_);
		const auto it = rx_channel_wrappers_.find(channel_name);
		if (it == rx_channel_wrappers_.end())
		{
			return nullptr;
		}
		const auto &info = it->second;
		return ([&info](const IMessageAck& ack)
		{
			info.acknowledge(ack);
		});
	}

	// Deprecated
#if 0
	void acknowledge(const std::string& channel_name, const IMessageAck& ack)
	{
		std::lock_guard<std::mutex> lock(mutex_);
		const auto it = rx_channel_wrappers_.find(channel_name);
		if (it == rx_channel_wrappers_.end())
		{
			return;
		}
		it->second.acknowledge(ack);
	}
#endif

	void setMaxTransmitBatchSize(const size_t size)
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (size >= 10) { max_tx_batch_size_ = size; }
		else { throw std::runtime_error("Max transmit batch size must be >= 10"); }
	}

	void triggerClose()
	{
		// TODO Need a check to sanity check that we haven't already requested a close
		// TODO Change this to be a lock on the notification queue later
		std::lock_guard<std::mutex> lock(mutex_);
		LOG_INFO("Triggering close");
		char close_char = 'C';
		if (int num_tx = send(notification_pipe[1], &close_char, 1, 0); num_tx > 0)
		{
			LOG_INFO("Sent close request");
		}
		else
		{
			LOG_FATAL("Failed to send close request to the notification pipe. Something has seriously gone wrong");
		}
	}

private:

	void cleanUpAmqpConnection_()
	{
		for (const auto& event : events_)
		{
			event_free(event);
		}
		events_.clear();
		evutil_closesocket(notification_pipe[0]);
		evutil_closesocket(notification_pipe[1]);

		connection_.reset();
		handler_.reset();
		event_base_free(evbase_);
	}

	void establishAmqpConnection_()
	{
		// Core AMQP connection bits
		evbase_ = event_base_new();
		handler_ = std::make_unique<MyLibEventHandler>(evbase_, events_);
		connection_ = std::make_unique<AMQP::TcpConnection>(handler_.get(), AMQP::Address(address_));

		// Create a notification pipe to talk to libevent
		if (evutil_socketpair(AF_UNIX, SOCK_STREAM, 0, notification_pipe) < 0) {
			throw std::runtime_error("Failed to create notification pipe");
		}
		evutil_make_socket_nonblocking(notification_pipe[0]);
		evutil_make_socket_nonblocking(notification_pipe[1]);

		// Create event for the read end of the pipe - you write to notification_pipe[1]
		auto notification_event = event_new(evbase_, notification_pipe[0],
											EV_READ | EV_PERSIST, &MyAmqpController::notification_callback_, this);
		event_add(notification_event, nullptr);
		events_.push_back(notification_event);
	}

	static void notification_callback_(evutil_socket_t fd, short events, void* arg) {
		LOG_DEBUG("notification callback");

		if (arg == nullptr)
		{
			throw std::runtime_error("notification callback arg is null");
		}

		// There is no good way to check type safety using void*
		MyAmqpController* self = static_cast<MyAmqpController*>(arg);

		// Clear the pipe - we've been notified that there's data, so there probably is
		char buf[1024];
		if (int num_rx = recv(fd, buf, sizeof(buf), 0); num_rx > 0)
		{
			// Hard close request = 'C'
			if (std::ranges::find(buf, buf + num_rx, 'C') != buf + num_rx)
			{
				self->hard_close_();
			}

			// Acknowledgements before a soft close

			// If soft close
			// - Remaining transmits (happy to ignore batch limits now, but only as many transmits as are in the queue at present)
			// - Then soft close

			// Else
			// - Batch transmits
		}

	}

	/**
	 * This ensures that we remove everything from the event queue so that we can exit that cleanly and restart if needed
	 */
	void hard_close_()
	{
		// Remove additional locally created events (not amqp-cpp events) e.g. notification pipe, timer events for tx, etc.
		// TODO - consider doing this on connection->close()? That should get rid of all the duplicate code...
		for (const auto& event : events_)
		{
			event_del(event);
			event_free(event);
		}
		events_.clear();

		// Close the connection
		connection_->close();
	}

	// TODO This will become an event called on the event queue based on a trigger - either from a new timer event or the notification pipe
	void handleTransmitThreads()
	{
		// Current number of messages transmitted in a loop on the thread (too many messages at once can hog the CPU and starve other threads)
		size_t current_batch_size_ = 0;

		while (!transmit_exit.load())
		{
			if (cmd_transmit_active.load())
			{
				const auto old_batch_size = current_batch_size_;
				for (auto& fn : transmit_functions_)
				{
					if (cmd_transmit_active.load())
					{
						fn(current_batch_size_);
					}
				}
				if (old_batch_size == current_batch_size_ || current_batch_size_ > max_tx_batch_size_)
				{
					current_batch_size_ = 0;
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}
			}
			else
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}

		}
	}

	// Main connection
	std::string address_;
	struct event_base *evbase_;
	std::unique_ptr<MyLibEventHandler> handler_;
	std::unique_ptr<AMQP::TcpConnection> connection_;

	// Notification pipe setup
	evutil_socket_t notification_pipe[2];
	std::vector<event*> events_;

	std::thread maintain_connection_thread;
	// This indicates whether we should be trying to maintain connections or not
	std::atomic<bool> maintain_connection {true};
	std::atomic<int> num_reconnections {0};

	// Channel handling
	std::unordered_map<std::string, MyAmqpTxChannelInfo> tx_channel_wrappers_;
	std::unordered_map<std::string, MyAmqpRxChannelInfo> rx_channel_wrappers_;
	std::vector<std::function<void( size_t &batch_size)>> transmit_functions_ ;

	// Transmitting on multiple channels
	std::jthread transmitter_thread;
	std::atomic<bool> cmd_transmit_active {false};
	std::atomic<bool> res_transmit_active {false};
	std::atomic<bool> transmit_exit {true};

	// used to prevent the transmit side hogging all the processing time
	size_t max_tx_batch_size_ = 500;

	// Set this to true when we have finished with processing events and cleaned up
	std::atomic<bool> is_connection_finished_with {false};
	mutable std::mutex mutex_;
};

}
