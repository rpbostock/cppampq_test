#pragma once
#include <iostream>
#include <memory>

#include <amqpcpp.h>
#include <amqpcpp/address.h>
#include <amqpcpp/libevent.h>
#include <amqpcpp/table.h>

#include "my_lib_event_handler.hpp"


class MyAmqpController {
public:
	MyAmqpController()
	{
		evbase = event_base_new();
		handler = std::make_unique<MyLibEventHandler>(evbase);
		connection = std::make_unique<AMQP::TcpConnection>(handler.get(), AMQP::Address("amqp://guest:guest@localhost/"));
	}

	~MyAmqpControllerNoChannel()
	{
		connection->close();
		event_base_free(evbase);
	}

	bool isConnectionReady()
	{
		return handler->isReady();
	}

private:
	struct event_base *evbase;
	std::unique_ptr<AMQP::LibEventHandler> handler;
	std::unique_ptr<AMQP::TcpConnection> connection;
};
