#pragma once
#include <iostream>
#include <memory>

#include <atomic>
#include <amqpcpp/address.h>
#include <amqpcpp/libevent.h>
#include "channel_config.hpp"

#include "my_amqp_channel.hpp"

namespace rmq
{

class MyLibEventHandler final : public AMQP::LibEventHandler
{
public:
	explicit MyLibEventHandler(struct event_base *evbase) : LibEventHandler(evbase)
	{

	}

	~MyLibEventHandler() = default;

	void onReady(AMQP::TcpConnection *connection) override
	{
		// std::cout << "onReady - connection is now ready" << std::endl;
		is_ready_ = true;
	}

	void onError(TcpConnection *connection, const char *message) override
	{
		std::cout << "onError - connection error: " << message << std::endl;
		is_error_ = true;
		connection->close();
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
	explicit MyAmqpController(const std::string& address) : address_(address)
	{
		evbase_ = event_base_new();
		handler_ = std::make_unique<MyLibEventHandler>(evbase_);
		connection_ = std::make_unique<AMQP::TcpConnection>(handler_.get(), AMQP::Address(address_));
	}

	~MyAmqpController()
	{
		// std::cout << "MyAmqpController::~MyAmqpController()" << std::endl;

		// Indicate we're ready to finish. This stops the re-connection logic
		maintain_connection.store(false);

		// Close the connection - this should also stop the event handling loop when complete
		connection_->close();

		// Wait for the connection to be finished with
		while (!is_connection_finished_with.load())
		{
			// std::cout << "MyAmqpController::~MyAmqpController() - waiting for processing to complete" << std::endl;
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		maintain_connection_thread.join(); // TODO Probably don't need the flag above anymore
		// std::cout << "MyAmqpController::~MyAmqpController() - done" << std::endl;
	}

	std::string createTransmitChannel(const ChannelConfig& config, ChannelListenerPtr listener=std::make_shared<ChannelListener>())
	{
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

		tx_channel_wrappers_.emplace(channel_name, MyAmqpTxChannelInfo(std::move(tx_channel), listener, queue, config));
		return channel_name;
	}

	std::string createReceiveChannel(const ChannelConfig& config, ChannelListenerPtr listener=std::make_shared<ChannelListener>())
	{
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

		rx_channel_wrappers_.emplace(channel_name, MyAmqpRxChannelInfo(std::move(rx_channel), listener, queue, config));

		// Need a way to access bits in the future
		return channel_name;
	}
	
	bool isConnectionReady() const
	{
		return handler_->isReady();
	}

	bool isChannelReady(const std::string& channel_name)
	{
		const auto it = tx_channel_wrappers_.find(channel_name);
		if (it == tx_channel_wrappers_.end())
		{
			return false;
		}
		return it->second.listener()->isActive();
	}

	bool isConnectionError() const
	{
		return handler_->isError();
	}

	void start()
	{
		maintain_connection_thread = std::thread(&MyAmqpController::run, this);
	}


	void run()
	{
		while (maintain_connection.load())
		{
			event_base_dispatch(evbase_);

			if (maintain_connection.load())
			{
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
					channel_wrapper.second.setChannel(std::move(tx_channel));
				}

				// Reset the receivers TODO

				++num_reconnections;
				std::cout << "Finished reconnecting to " << address_ << std::endl;
			}
		}
		event_base_free(evbase_);

		// We need to let the deconstructor know that we are done with all the event bits
		is_connection_finished_with.store(true);
	}

	int getNumReconnections() const
	{
		return num_reconnections.load();
	}

	// TODO Handle channel errors
	void onChannelError(const std::string& channel_name, const char *message) const
	{
		// Force a reconnection and restart of all the channels - from experience just restarting the affected channel doesn't work
		LOG_ERROR(message);
		connection_->close();
	}

	MyTxDataQueuePtr getTxQueue(const std::string& channel_name)
	{
		const auto it = tx_channel_wrappers_.find(channel_name);
		if (it == tx_channel_wrappers_.end())
		{
			return nullptr;
		}
		return it->second.queue();
	}

	MyRxDataQueuePtr getRxQueue(const std::string& channel_name)
	{
		const auto it = rx_channel_wrappers_.find(channel_name);
		if (it == rx_channel_wrappers_.end())
		{
			return nullptr;
		}
		return it->second.queue();
	}

	void acknowledge(const std::string& channel_name, const IMessageAck& ack)
	{
		const auto it = rx_channel_wrappers_.find(channel_name);
		if (it == rx_channel_wrappers_.end())
		{
			return;
		}
		it->second.acknowledge(ack);
	}

private:
	// Main connection
	std::string address_;
	struct event_base *evbase_;
	std::unique_ptr<MyLibEventHandler> handler_;
	std::unique_ptr<AMQP::TcpConnection> connection_;

	std::thread maintain_connection_thread;
	// This indicates whether we should be trying to maintain connections or not
	std::atomic<bool> maintain_connection {true};
	std::atomic<int> num_reconnections {0};

	// Channel handling
	std::unordered_map<std::string, MyAmqpTxChannelInfo> tx_channel_wrappers_;
	std::unordered_map<std::string, MyAmqpRxChannelInfo> rx_channel_wrappers_;

	// Set this to true when we have finished with processing events and cleaned up
	std::atomic<bool> is_connection_finished_with {false};
};

}
