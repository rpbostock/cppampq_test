#pragma once
#include <iostream>
#include <memory>

#include <amqpcpp.h>
#include <amqpcpp/address.h>
#include <amqpcpp/libevent.h>
#include <amqpcpp/table.h>

#include "my_lib_event_handler.hpp"

using namespace AMQP;

class MyAmqpControllerNoChannel
{
public:
	MyAmqpControllerNoChannel()
	{
		evbase = event_base_new();
		handler = std::make_unique<MyLibEventHandler>(evbase);
		connection = std::make_unique<AMQP::TcpConnection>(handler.get(), AMQP::Address("amqp://guest:guest@localhost/"));
	}
	~MyAmqpControllerNoChannel()
	{

	}

	void run()
	{
		event_base_dispatch(evbase);
		event_base_free(evbase);
	}

private:
	struct event_base *evbase;
	std::unique_ptr<AMQP::LibEventHandler> handler;
	std::unique_ptr<AMQP::TcpConnection> connection;
};
