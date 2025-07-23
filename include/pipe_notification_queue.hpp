#pragma once
#include <memory>
#include <event2/util.h>

#include "queue.hpp"

// We need a thread safe mechanism of talking to libevent
class NotificationPipeTransmitter
{
public:
	explicit NotificationPipeTransmitter(evutil_socket_t notification_pipe, std::atomic<bool>& is_processed) : notification_pipe_(notification_pipe), is_processed_(is_processed) {}

	bool notify(const char cmd)
	{
		std::lock_guard<std::mutex> lock(mutex_);

		// To prevent the buffer being overloaded when we're transmitting a lot we need a mechanism of throttling what is sent
		if (is_processed_.load())
		{
			chars_processing_.clear();
			is_processed_.store(false);
		}
		else
		{
			if (chars_processing_.find(cmd) != chars_processing_.end())
			{
				LOG_TRACE("Command already in the processing queue");
				return false;
			}
		}

		if (const int num_tx = send(notification_pipe_, &cmd, 1, 0); num_tx > 0)
		{
			LOG_DEBUG("Sent command: " << cmd );
			chars_processing_.insert(cmd);
			return true;
		}
		else
		{
			// This should not happen now that we have throttling in place
			LOG_WARN("Failed to send command to the notification pipe");
			return false;
		}
	}
private:
	std::atomic<bool>& is_processed_;
	std::set<char> chars_processing_;
	evutil_socket_t notification_pipe_ {-1};
	std::mutex mutex_;
};

/**
 * Provide a queue variant that will allow us to notify RMQ via a pipe notification when
 * we add new data to the queue.
 *
 * Typical usage:
 * - Declare the queue
 * - Add data to the queue
 *
 * @tparam Typ
 */
template<typename Typ>
class PipeNotificationQueue final : public Queue<Typ> {
public:
	explicit PipeNotificationQueue(size_t max_size
	                               , QueueOverflowPolicy default_overflow_policy = QueueOverflowPolicy::WAIT
	                               , const std::string& name = "queue") : Queue<Typ>(max_size, default_overflow_policy, name)
	{}

	void setupNotificationPipe(const char notification_cmd, const std::shared_ptr<NotificationPipeTransmitter>  &notification_pipe)
	{
		LOG_DEBUG("Setting up notification pipe for command: " << notification_cmd);
		notification_cmd_ = notification_cmd;
		notification_pipe_ = notification_pipe;
	}

	void push(const Typ& obj) override
	{
		LOG_TRACE("Pushing object to queue: " << obj);
		Queue<Typ>::push(obj);
		notify();
	}

	void push(Typ&& obj) override
	{
		LOG_TRACE("Pushing object to queue: " << obj);
		Queue<Typ>::push(std::move(obj));
		notify();
	}

	void notify() const
	{
		if (notification_pipe_)
		{
			notification_pipe_->notify(notification_cmd_);
		}
	}


private:
	char notification_cmd_ {0x00};
	std::shared_ptr<NotificationPipeTransmitter> notification_pipe_ {nullptr};
};
