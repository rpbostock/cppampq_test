#pragma once
#include <memory>

#include <atomic>
#include <ranges>
#include <thread>
#include <amqpcpp/address.h>
#include <amqpcpp/libevent.h>
#include "channel_config.hpp"

#include "my_amqp_channel.hpp"
#include "tx_client_wrapper.hpp"
#include "rx_client_wrapper.hpp"

namespace rmq
{

class EventsMap : public std::map<std::string, event*>
{
public:
	void cleanup()
	{
		for (const auto& event : *this)
		{
			event_del(event.second);
			event_free(event.second);
		}
		clear();
	}
};

class MyLibEventHandler final : public AMQP::LibEventHandler
{
public:
	explicit MyLibEventHandler(struct event_base *evbase, EventsMap &events) : LibEventHandler(evbase), events_(events)
	{

	}

	~MyLibEventHandler()
	{
		events_.cleanup();
	}

	void onReady(AMQP::TcpConnection *connection) override
	{
		// std::cout << "onReady - connection is now ready" << std::endl;
		is_ready_ = true;
	}

	void onError(AMQP::TcpConnection *connection, const std::string& message)
	{
		onError(connection, message.c_str());
	}

	void onError(AMQP::TcpConnection *connection, const char *message) override
	{
		LOG_ERROR("onError - connection error: " << message);
		is_error_ = true;
		events_.cleanup();
		connection->close(); // Suspect this may not be needed on an error, but just to be safe...
	}

	void onClosed(AMQP::TcpConnection *connection) override
	{
		LOG_DEBUG("onClosed - connection closed and clearing up events");
		events_.cleanup();
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
	MyLibEventHandler() = delete;

	EventsMap &events_;
	std::atomic<bool> is_ready_ {false};
	std::atomic<bool> is_error_ {false};
};



using ChannelHandlerTxPtr = std::unique_ptr<MyAmqpTxChannel>;
using ChannelHandlerRxPtr = std::unique_ptr<MyAmqpRxChannel>;
template <typename MessageType, typename ChannelTypePtr>
class MyAmqpChannelInfo
{
public:
	MyAmqpChannelInfo( const ChannelListenerPtr &listener
		, const std::shared_ptr<MessageType>& queue
		, const ChannelConfig& config)
		: listener_(listener)
		, queue_(queue)
		, config_(config)
		, channel_(nullptr)
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

private:
	ChannelListenerPtr listener_;
	std::shared_ptr<MessageType> queue_;
	// ReSharper disable once CppDFANotInitializedField
	const ChannelConfig config_;
	ChannelTypePtr channel_;
};


class MyAmqpTxChannelInfo : public MyAmqpChannelInfo<MyTxDataQueue, ChannelHandlerTxPtr>
{
public:
	MyAmqpTxChannelInfo(const ChannelListenerPtr &listener
	                             , const std::shared_ptr<MyTxDataQueue> &queue
	                             , const ChannelConfig &config) :
		MyAmqpChannelInfo(listener, queue, config)
	{
	}
};

class MyAmqpRxChannelInfo : public MyAmqpChannelInfo<MyRxDataQueue, ChannelHandlerRxPtr>
{
public:
	explicit MyAmqpRxChannelInfo(const ChannelListenerPtr &listener
	                             , const MyRxDataQueuePtr &data_queue
	                             , const MyRxAckQueuePtr &ack_queue
	                             , const ChannelConfig &config) :
		MyAmqpChannelInfo(listener, data_queue, config)
	, ack_queue_(ack_queue)
	{
	}

	MyRxAckQueuePtr getAckQueue()
	{
		return ack_queue_;
	}
private:

	MyRxAckQueuePtr ack_queue_;
};

class MyAmqpController {
public:
	using TxChannelHandlerPtr = std::shared_ptr<MyAmqpTxChannel>;
	MyAmqpController(const std::string &address) : address_(address)
	                                                        , evbase_(nullptr)
	                                                        , notification_pipe{-1, -1}
	{
	}

	~MyAmqpController()
	{
		LOG_TRACE("MyAmqpController::~MyAmqpController()");
		if (maintain_connection.load())
		{
			maintain_connection.store(false);
			triggerClose();
		}

		maintain_connection_thread.join();
		LOG_TRACE("MyAmqpController::~MyAmqpController() - done");
	}

	std::string getAddress() const { return address_; }

	TxClientWrapper createTransmitChannel(const ChannelConfig& config, ChannelListenerPtr listener=std::make_shared<SimpleChannelListener>())
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (!listener)
		{
			throw std::runtime_error("Cannot accept nullptr listener");
		}
		if (maintain_connection.load())
		{
			throw std::runtime_error("Cannot create transmit channel while maintaining connection");
		}

		auto channel_name = config.exchange_name;
		const auto queue = std::make_shared<MyTxDataQueue>(1000, QueueOverflowPolicy::WAIT); // possibly pass this in as a parameter?
		tx_channel_wrappers_.emplace(channel_name, MyAmqpTxChannelInfo(listener, queue, config));

		return TxClientWrapper(channel_name, listener, queue);
	}


	RxClientWrapper createReceiveChannel(const ChannelConfig& config, ChannelListenerPtr listener=std::make_shared<SimpleChannelListener>())
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (!listener)
		{
			throw std::runtime_error("Cannot accept nullptr listener");
		}
		if (maintain_connection.load())
		{
			throw std::runtime_error("Cannot create receive channel while maintaining connection");
		}

		auto channel_name = config.queue_name;
		auto data_queue = std::make_shared<MyRxDataQueue>(1000, QueueOverflowPolicy::WAIT); // possibly pass this in as a parameter?
		auto ack_queue = std::make_shared<MyRxAckQueue>(1000, QueueOverflowPolicy::WAIT); // possibly pass this in as a parameter?
		rx_channel_wrappers_.emplace(channel_name, MyAmqpRxChannelInfo(listener, data_queue, ack_queue, config));

		// Need a way to access bits in the future
		return RxClientWrapper(channel_name, listener, data_queue, ack_queue);
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
		// Always start off trying to maintain the connection
		maintain_connection.store(true);

		while (maintain_connection.load())
		{
			establishAmqpConnection_();

			// This will only return once all registered event sources (controller and channels) have finished
			assert(evbase_!=nullptr);
			event_base_dispatch(evbase_);

			cleanUpAmqpConnection_();
		}
	}

	int getNumReconnections() const
	{
		const int num_reconnections = std::max(0, num_connections.load() - 1);
		LOG_DEBUG("Number of reconnections: " << num_reconnections);
		return num_reconnections;
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


	void setMaxTransmitBatchSize(const size_t size)
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (size >= 10) { max_tx_batch_size_ = size; }
		else { throw std::runtime_error("Max transmit batch size must be >= 10"); }
	}

	void triggerClose() const
	{
		LOG_INFO("Triggering close");
		if (notification_pipe_transmitter_ && notification_pipe_transmitter_->notify('C'))
		{
			LOG_INFO("Sent close request");
		}
		else
		{
			LOG_WARN("Failed to send close request to the notification pipe. Probably we've already tried to send a close");
		}
	}

private:
	/**
	 * We only get to this stage when the event thread has already returned. This means:
	 * - TCPConnection has closed
	 * - Any active transmits have already been completed
	 * - The receive side of the notification pipe isn't being monitored
	 */
	void cleanUpAmqpConnection_()
	{
		if (nullptr == evbase_)
		{
			throw std::runtime_error("cleanUpAmqpConnection_ called before establishAmqpConnection_");
		}

		// Anything to do with TX channels here!
		notification_pipe_transmitter_.reset(); // We don't want to accidentally try and send any data across
		for (auto &val: tx_channel_wrappers_ | std::views::values)
		{
			val.resetChannel();
			val.queue()->setupNotificationPipe('X', nullptr);
			val.queue()->flush();
		}
		for (auto &val: rx_channel_wrappers_ | std::views::values)
		{
			val.resetChannel();
			val.getAckQueue()->setupNotificationPipe('X', nullptr);
			val.queue()->flush();
			val.getAckQueue()->flush();
		}


		events_.cleanup(); // This should already have occurred otherwise the event loop wouldn't exit, but shouldn't harm anything
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
		if (nullptr == evbase_)
		{
			throw std::runtime_error("Failed to create event base");
		}

		handler_ = std::make_unique<MyLibEventHandler>(evbase_, events_);
		connection_ = std::make_unique<AMQP::TcpConnection>(handler_.get(), AMQP::Address(address_));

		// Create a notification pipe to talk to libevent
		if (evutil_socketpair(AF_UNIX, SOCK_STREAM, 0, notification_pipe) < 0)
		{
			throw std::runtime_error("Failed to create notification pipe");
		}
		evutil_make_socket_nonblocking(notification_pipe[0]);
		evutil_make_socket_nonblocking(notification_pipe[1]);
		notification_pipe_transmitter_ = std::make_shared<NotificationPipeTransmitter>(notification_pipe[1], is_pipe_processed_);

		// Create event for the read end of the pipe - you write to notification_pipe[1]
		auto notification_event = event_new(evbase_, notification_pipe[0],
											EV_READ | EV_PERSIST, &MyAmqpController::notification_callback_, this);
		event_add(notification_event, nullptr);
		events_.emplace(pipe_event_name, notification_event);

		// Any transmit channels
		for (auto&[name, info] : tx_channel_wrappers_)
		{
			LOG_INFO("Creating TX channel " << name);
			auto tx_channel = std::make_unique<MyAmqpTxChannel>(
			// Create a new channel and connect it to the new connection
				connection_.get()
				, info.config()
				, [handler = handler_.get()](AMQP::TcpConnection* conn, const std::string& error) { handler->onError(conn, error); }
				, [this]() { this->handleTransmitThreads_(); }
				, info.queue()
				, info.listener()
				, info.listener()->getNumberOfTransmittedMessages()
				);

			// Connect the queue to the notification pipe
			info.queue()->setupNotificationPipe('t', notification_pipe_transmitter_);
			transmit_functions_.emplace_back(std::bind(&MyAmqpTxChannel::sendData, tx_channel.get(), std::placeholders::_1));
			info.setChannel(std::move(tx_channel));
		}

		// Any receive channels
		for (auto&[channel_name, info] : rx_channel_wrappers_)
		{
			LOG_INFO("Creating RX channel " << channel_name);
			auto rx_channel = std::make_unique<MyAmqpRxChannel>(
				connection_.get()
				, info.config()
				, [handler = handler_.get()](AMQP::TcpConnection* conn, const std::string& error) { handler->onError(conn, error); }
				, [this]() { this->handleTransmitThreads_(); }
				, info.queue()
				, info.getAckQueue()
				, info.listener()
				, info.listener()->getNumberOfReceivedMessages()
				, info.listener()->getNumberOfAcknowledgedMessages()
			);

			// Connect the queue to the notification pipe
			info.getAckQueue()->setupNotificationPipe('t', notification_pipe_transmitter_);
			transmit_functions_.emplace_back(std::bind(&MyAmqpRxChannel::sendAck, rx_channel.get(), std::placeholders::_1));
			info.setChannel(std::move(rx_channel));
		}

		++num_connections;
		LOG_INFO("Finished connecting to " << address_ << ". Number of reconnections: " << num_connections - 1);
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
		char buf[10000];
		if (int num_rx = recv(fd, buf, sizeof(buf), 0); num_rx > 0)
		{
			self->is_pipe_processed_.store(true);
			LOG_DEBUG("Received " << num_rx << " bytes from notification pipe");
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
			if (std::ranges::find(buf, buf + num_rx, 't') != buf + num_rx)
			{
				self->handleTransmitThreads_();
			}
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
			event_del(event.second);
			event_free(event.second);
		}
		events_.clear();

		// Close the connection
		connection_->close();
	}


	void handleTransmitThreads_()
	{
		// Current number of messages transmitted in a loop on the thread (too many messages at once can hog the CPU and starve other threads)
		size_t current_batch_size_ = 0;
		auto old_batch_size = 1;
		while (current_batch_size_ <= max_tx_batch_size_ && current_batch_size_ != old_batch_size)
		{
			old_batch_size = current_batch_size_;
			for (auto& fn : transmit_functions_)
			{
				// TODO Add a check for an error here between each call
				fn(current_batch_size_);
			}
		}
		if (current_batch_size_ == old_batch_size)
		{
			LOG_TRACE("Finished transmit - sent " << current_batch_size_ << " messages");
			return;
		}
		if (current_batch_size_ > max_tx_batch_size_)
		{
			LOG_DEBUG("Too many messages in queue. Transmitted " << current_batch_size_ << ". Scheduling another transmit.");

			// Need to use an event to trigger this function again. This variant automatically cleans up the related event
			struct timeval tv = {0, 10000};  // Timeout has not yet been tuned. We need enough to free up other threads, but as little as possible not to significantly impact the tx rate
			event_base_once(evbase_, -1, EV_TIMEOUT,
				[](evutil_socket_t fd, short what, void* arg) {
					auto self = static_cast<MyAmqpController*>(arg);
					LOG_DEBUG("Processing handleTransmitThreads_ from callback");
					self->handleTransmitThreads_();
				},
				this, &tv);
		}
	}

	// Main connection
	std::string address_;
	// cppcheck-suppress uninitMemberVar
	struct event_base *evbase_;
	std::unique_ptr<MyLibEventHandler> handler_;
	std::unique_ptr<AMQP::TcpConnection> connection_;

	// Notification pipe setup
	evutil_socket_t notification_pipe[2];
	EventsMap events_; // TODO Do we need this here - better in the MyLibEventHandler if possible
	const std::string pipe_event_name = "notification_pipe";
	const std::string transmit_event = "notification_pipe";
	std::atomic<bool> is_pipe_processed_ {true}; // Flag indicating whether new data has been added
	std::shared_ptr<NotificationPipeTransmitter> notification_pipe_transmitter_;

	std::thread maintain_connection_thread;
	// This indicates whether we should be trying to maintain connections or not
	std::atomic<bool> maintain_connection {false};
	std::atomic<int> num_connections {0};

	// Channel handling
	std::unordered_map<std::string, MyAmqpTxChannelInfo> tx_channel_wrappers_;
	std::unordered_map<std::string, MyAmqpRxChannelInfo> rx_channel_wrappers_;
	std::vector<std::function<void( size_t &batch_size)>> transmit_functions_ ;

	// used to prevent the transmit side hogging all the processing time
	size_t max_tx_batch_size_ = 500;

	// Set this to true when we have finished with processing events and cleaned up
	std::atomic<bool> is_connection_finished_with {false};
	mutable std::mutex mutex_;
};

}
