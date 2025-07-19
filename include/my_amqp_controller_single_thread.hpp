#pragma once

#include <iostream>
#include <memory>

#include <amqpcpp/address.h>
#include <amqpcpp/libevent.h>

using namespace AMQP;

class MySTLibEventHandler : public AMQP::LibEventHandler
{
public:
	MySTLibEventHandler(struct event_base *evbase) : LibEventHandler(evbase)
	{

	}

	virtual ~MySTLibEventHandler()
	{

	}

	void onReady(AMQP::TcpConnection *connection) override
	{
		std::cout << "onReady" << std::endl;
		is_ready_.store(true);
	}

	bool isReady()
	{
		return is_ready_.load() ;
	}
private:
	std::atomic<bool> is_ready_{false};

};

class MyAmqpControllerSingleThread {
public:
    MyAmqpControllerSingleThread() {};
    ~MyAmqpControllerSingleThread() {};

    void triggerCloseEvent() {
        std::cout << "triggerCloseEvent" << std::endl;
        // Thread-safe way to notify the event loop
        char buf = 1;
        if (send(notification_pipe[1], &buf, 1, 0) != 1) {
            std::cerr << "Failed to write to notification pipe" << std::endl;
        }
    }

	bool isConnectionReady() const
    {
    	if (!handler) { return false;}
    	return handler->isReady();
    }


	void run()
    {
    	evbase = event_base_new();
    	handler = std::make_unique<MySTLibEventHandler>(evbase);
    	connection = std::make_unique<AMQP::TcpConnection>(handler.get(), AMQP::Address("amqp://guest:guest@localhost/"));

    	// Create a notification pipe
    	if (evutil_socketpair(AF_UNIX, SOCK_STREAM, 0, notification_pipe) < 0) {
    		throw std::runtime_error("Failed to create notification pipe");
    	}
    	evutil_make_socket_nonblocking(notification_pipe[0]);
    	evutil_make_socket_nonblocking(notification_pipe[1]);

    	// Create event for the read end of the pipe
    	close_event = event_new(evbase, notification_pipe[0],
			EV_READ | EV_PERSIST, &MyAmqpControllerSingleThread::close_callback, this);
    	event_add(close_event, nullptr);

    	event_base_dispatch(evbase);

    	event_free(close_event);
    	evutil_closesocket(notification_pipe[0]);
    	evutil_closesocket(notification_pipe[1]);
    	event_base_free(evbase);
    }

private:
    static void close_callback(evutil_socket_t fd, short events, void* arg) {
        std::cout << "close_callback" << std::endl;
        auto* self = static_cast<MyAmqpControllerSingleThread*>(arg);

        // Clear the pipe
        char buf[1024];
    	int num_rx = 0;
        while (recv(fd, buf, sizeof(buf), 0) > 0 && num_rx < 1)
        {
	        ++num_rx;
        }

    	std::cout << "close_callback - num_rx: " << num_rx << std::endl;
        self->connection->close();
    	event_del(self->close_event);
    }

    struct event_base *evbase;
    struct event *close_event{nullptr};
    evutil_socket_t notification_pipe[2];
    std::unique_ptr<MySTLibEventHandler> handler;
    std::unique_ptr<AMQP::TcpConnection> connection;
};