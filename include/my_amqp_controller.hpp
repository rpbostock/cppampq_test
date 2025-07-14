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

class MyAmqpController {
public:
	using ChannelHandlerPtr = std::shared_ptr<MyAmqpChannel>;
	using TxChannelHandlerPtr = std::shared_ptr<MyAmqpTxChannel>;
	// using RxChannelHandlerPtr = std::shared_ptr<MyAmqpRxChannel>;
	using TxChannelDataHandlerPtr = std::shared_ptr<MyAmqpTxChannelDataHandler>;
	// using RxChannelDataHandlerPtr = std::shared_ptr<MyAmqpRxChannelDataHandler>;
	explicit MyAmqpController(const std::string& address) : address_(address)
	{
		evbase = event_base_new();
		handler = std::make_unique<MyLibEventHandler>(evbase);
		connection = std::make_unique<AMQP::TcpConnection>(handler.get(), AMQP::Address(address_));
	}

	~MyAmqpController()
	{
		// std::cout << "MyAmqpController::~MyAmqpController()" << std::endl;

		// Indicate we're ready to finish. This stops the re-connection logic
		maintain_connection.store(false);

		// Close the connection - this should also stop the event handling loop when complete
		connection->close();

		// Wait for the connection to be finished with
		while (!is_connection_finished_with.load())
		{
			// std::cout << "MyAmqpController::~MyAmqpController() - waiting for processing to complete" << std::endl;
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		maintain_connection_thread.join(); // TODO Probably don't need the flag above anymore
		// std::cout << "MyAmqpController::~MyAmqpController() - done" << std::endl;
	}

	TxChannelHandlerPtr createTransmitChannel(const ChannelConfig& config)
	{
		auto amqp_channel = std::make_unique<AMQP::TcpChannel>(connection.get());
		handlers_.emplace(config.queue_name, std::make_shared<MyAmqpTxChannelDataHandler>());
		auto tx_channel = std::make_shared<MyAmqpTxChannel>(std::move(amqp_channel)
			, config
			, [&config, this](const std::string &error_message) { onChannelError(config.queue_name, error_message.c_str()); }
			, handlers_[config.queue_name]);
		channels_.emplace(config.queue_name, tx_channel);
		return tx_channel;
	}



	// RxChannelHandlerPtr getChannelRx(const std::string& channel_name)
	// {
	// 	const auto it = channels_.find(channel_name);
	// 	if (it == channels_.end())
	// 		return RxChannelHandlerPtr();
	// 	return std::dynamic_pointer_cast<MyAmqpRxChannel<std::any>>(it->second);
	// }

	TxChannelHandlerPtr getChannelTx(const std::string& channel_name)
	{
		const auto it = channels_.find(channel_name);
		if (it == channels_.end())
			return TxChannelHandlerPtr();
		return std::dynamic_pointer_cast<MyAmqpTxChannel>(it->second);
	}



	bool isConnectionReady() const
	{
		return handler->isReady();
	}

	bool isConnectionError() const
	{
		return handler->isError();
	}

	void start()
	{
		maintain_connection_thread = std::thread(&MyAmqpController::run, this);
	}


	void run()
	{
		while (maintain_connection.load())
		{
			event_base_dispatch(evbase);

			if (maintain_connection.load())
			{
				// Set up a new connection
				std::cout << "Reconnecting to " << address_ << std::endl;
				handler = std::make_unique<MyLibEventHandler>(evbase);
				connection = std::make_unique<AMQP::TcpConnection>(handler.get(), AMQP::Address(address_));
				++num_reconnections;
				std::cout << "Finished reconnecting to " << address_ << std::endl;
			}
		}
		event_base_free(evbase);

		// We need to let the deconstructor know that we are done with all the event bits
		is_connection_finished_with.store(true);
	}

	int getNumReconnections() const
	{
		return num_reconnections.load();
	}

	// TODO Handle channel errors
	void onChannelError(std::string channel_name, const char *message)
	{
		// Check if the connection has an error or not - if we do then we need to handle that and not this
		if (isConnectionError())
		{
			LOG_ERROR("Already handling connection error when we received a channel error: " + std::string(message) + " on channel " + channel_name);
		}
		else
		{

		}
	}

private:
	// Main connection
	std::string address_;
	struct event_base *evbase;
	std::unique_ptr<MyLibEventHandler> handler;
	std::unique_ptr<AMQP::TcpConnection> connection;

	std::thread maintain_connection_thread;
	// This indicates whether we should be trying to maintain connections or not
	std::atomic<bool> maintain_connection {true};
	std::atomic<int> num_reconnections {0};

	// Channel handling
	std::unordered_map<std::string, ChannelHandlerPtr> channels_;
	std::unordered_map<std::string, TxChannelDataHandlerPtr> handlers_;

	// Set this to true when we have finished with processing events and cleaned up
	std::atomic<bool> is_connection_finished_with {false};
};

}
