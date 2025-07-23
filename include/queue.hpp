#pragma once

#include <boost/circular_buffer.hpp>

#include <condition_variable>
#include <mutex>
#include <queue>

#include "logging.hpp"

#ifndef WARN_UNUSED_RESULT
#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
#endif

#ifndef IGNORE_RESULT
#define IGNORE_RESULT(x) do { auto __rc = (x); } while (false)
#endif


int static queue_id = 0;
enum class QueueOverflowPolicy { WAIT, DISCARD_OLDEST, DISCARD_NEWEST };

template<typename Typ>
class Queue
{
public:
	virtual ~Queue() = default;
	explicit Queue(size_t max_size, QueueOverflowPolicy default_overflow_policy = QueueOverflowPolicy::WAIT, const std::string& name = "queue")
		:default_overflow_policy_(default_overflow_policy)
		,max_size_(max_size)
		,queue_(boost::circular_buffer<Typ>(max_size))
		,name_(name + std::to_string(++queue_id))
	{}

	WARN_UNUSED_RESULT bool push(const Typ& obj, QueueOverflowPolicy overflow_policy)
	{
		std::unique_lock<std::mutex> lock(mutex_);
		if(!handleFull(lock, overflow_policy))
		{
			return false;
		}

		queue_.push(obj);
		event_.notify_one();
		return true;
	}

	WARN_UNUSED_RESULT bool push(Typ&& obj, QueueOverflowPolicy overflow_policy)
	{
		std::unique_lock<std::mutex> lock(mutex_);
		if(!handleFull(lock, overflow_policy))
		{
			return false;
		}
		queue_.push(std::move(obj));
		event_.notify_one();
		return true;
	}

	virtual void push(const Typ& obj)
	{
		IGNORE_RESULT(push(obj, default_overflow_policy_));
	}

	virtual void push(Typ&& obj)
	{
		IGNORE_RESULT(push(std::move(obj), default_overflow_policy_));
	}

	Typ pop(void)
	{
		std::unique_lock<std::mutex> lock(mutex_);
		while(queue_.empty())
		{
			event_.wait_for(lock, std::chrono::milliseconds(TIMEOUT_MS));
		}

		Typ result = std::move(queue_.front());
		queue_.pop();
		event_.notify_one();
		return result;
	}

	Typ& peek()
	{
		std::unique_lock<std::mutex> lock(mutex_);
		while(queue_.empty())
		{
			event_.wait_for(lock, std::chrono::milliseconds(TIMEOUT_MS));
		}

		Typ& result = queue_.front();
		return result;
	}

	bool isEmpty()
	{
		std::unique_lock<std::mutex> lock(mutex_);
		return queue_.empty();
	}

	bool isFull()
	{
		std::unique_lock<std::mutex> lock(mutex_);
		return queue_.size() >= max_size_;
	}

	void flush()
	{
		std::unique_lock<std::mutex> lock(mutex_);
		while(!queue_.empty())
		{
			queue_.pop();
		}
	}

	/**!
	 * Resizes the queue - warning that this can discard data if reducing the size of the queue
	 * @param size
	 */
	void resize(size_t size)
	{
		std::unique_lock<std::mutex> lock(mutex_);
		max_size_ = size;
		queue_.c.resize(size);
	}

	size_t size()
	{
		std::unique_lock<std::mutex> lock(mutex_);
		return queue_.size();
	}
private:
	WARN_UNUSED_RESULT bool handleFull(std::unique_lock<std::mutex>& lock, QueueOverflowPolicy overflow_policy)
	{
		while(queue_.size() >= max_size_)
		{
			switch(overflow_policy)
			{
			case QueueOverflowPolicy::WAIT:
			{
				auto cv = event_.wait_for(lock, std::chrono::milliseconds(TIMEOUT_MS));
				if (cv == std::cv_status::timeout)
				{
					LOG_DEBUG("Timeout while writing to queue " << name_);
				}
				break;
			}
			case QueueOverflowPolicy::DISCARD_OLDEST:
				{
					LOG_TRACE("DISCARD_OLDEST, queue: " << name_);
					queue_.pop();
					break;
				}

			case QueueOverflowPolicy::DISCARD_NEWEST:
				{
					LOG_TRACE("DISCARD_NEWEST, queue: " << name_);
					return false;
				}

			}
		}
		return true;
	}

	enum { TIMEOUT_MS = 100 };
	Queue(const Queue<Typ>& other) = delete;
	void operator=(const Queue<Typ>& other) = delete;

	QueueOverflowPolicy default_overflow_policy_;
	std::size_t max_size_;
	std::string name_;
	std::mutex mutex_;
	std::condition_variable event_;
	std::queue<Typ, boost::circular_buffer<Typ> > queue_;
};

