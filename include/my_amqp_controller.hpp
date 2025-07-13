#pragma once
#include <iostream>
#include <memory>

#include <amqpcpp.h>
#include <atomic>
#include <amqpcpp/address.h>
#include <amqpcpp/libevent.h>
#include <amqpcpp/table.h>

namespace rmq
{

class MyLibEventHandler : public AMQP::LibEventHandler
{
public:
	MyLibEventHandler(struct event_base *evbase) : LibEventHandler(evbase)
	{

	}

	virtual ~MyLibEventHandler()
	{
		std::cout << "MyLibEventHandler::~MyLibEventHandler()" << std::endl;
	}

	void onReady(AMQP::TcpConnection *connection) override
	{
		std::cout << "onReady - connection is now ready" << std::endl;
		is_ready_ = true;
	}

	bool isReady() const
	{
		return is_ready_.load();
	}

private:
	std::atomic<bool> is_ready_ {false};

};

class MyAmqpController {
public:
	MyAmqpController()
	{
		evbase = event_base_new();
		handler = std::make_unique<MyLibEventHandler>(evbase);
		connection = std::make_unique<AMQP::TcpConnection>(handler.get(), AMQP::Address("amqp://guest:guest@localhost/"));
	}

	~MyAmqpController()
	{
		std::cout << "MyAmqpController::~MyAmqpController()" << std::endl;
		connection->close();
		while (!is_processing_complete.load())
		{
			std::cout << "MyAmqpController::~MyAmqpController() - waiting for processing to complete" << std::endl;
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		std::cout << "MyAmqpController::~MyAmqpController() - done" << std::endl;
	}

	bool isConnectionReady()
	{
		return handler->isReady();
	}

	void run()
	{
		event_base_dispatch(evbase);
		event_base_free(evbase);

		// We need to let the deconstructor know that we are done with all the event bits
		is_processing_complete.store(true);
	}

private:
	struct event_base *evbase;
	std::unique_ptr<MyLibEventHandler> handler;
	std::unique_ptr<AMQP::TcpConnection> connection;
	std::atomic<bool> is_processing_complete {false};
};

}