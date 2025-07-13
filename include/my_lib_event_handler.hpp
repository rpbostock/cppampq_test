#pragma once
#include <iostream>
#include <ostream>

#include "amqpcpp/libevent.h"

class MyLibEventHandler : public AMQP::LibEventHandler
{
public:
	MyLibEventHandler(struct event_base *evbase) : LibEventHandler(evbase)
	{

	}

	virtual ~MyLibEventHandler()
	{

	}

	void onReady(AMQP::TcpConnection *connection) override
	{
		std::cout << "onReady - planning to close now" << std::endl;
		connection->close();
	}

};
