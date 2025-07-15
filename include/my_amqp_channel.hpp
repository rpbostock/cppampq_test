#pragma once
#include "queue.hpp"
#include "logging.hpp"
#include "my_amqp_controller.hpp"
#include "my_amqp_controller.hpp"

namespace rmq {

enum class ChannelState { none, initialising, active, deactivating, inactive, error };
using MyTxDataQueue = Queue<std::shared_ptr<std::vector<char>>>;
using MyTxDataQueuePtr = std::shared_ptr<Queue<std::shared_ptr<std::vector<char>>>>;

class ChannelListener
{
public:
	void onNumberOfTransmittedMessages(size_t num_transmitted)
	{
		if (num_transmitted%100000)
		{

		}
		num_transmitted_ = num_transmitted;
	}

	void onChannelStateChange(const ChannelState state)
	{
		LOG_INFO("Channel state changed to " << static_cast<int>(state));
		state_.store(state);
	}

	size_t getNumberOfTransmittedMessages() const
	{
		return num_transmitted_.load();
	}

	bool isActive() const
	{
		return state_.load() == ChannelState::active;
	}
private:
	std::atomic<size_t> num_transmitted_ {0};
	std::atomic<ChannelState> state_ {ChannelState::none};
};

/**
 * This class will handle the specific parts of AMQP relating to the channel
 * itself
 */
class MyAmqpChannel
{
public:

	MyAmqpChannel(const ChannelConfig& channel_config
		, std::function<void(const std::string &)> error_callback) :
		channel_config_(channel_config)
		, on_error_callback_(std::move(error_callback))
	{}
	virtual ~MyAmqpChannel() = default;
	virtual void deactivate() = 0;


protected:
	/**
	 * This is called when constructed or after handling an error
	 */
	virtual void setupBaseTcpChannel()
	{
		std::lock_guard lock(tcp_channel_mutex_);
		if (!tcp_channel_)
		{
			throw std::runtime_error("TCP channel is not set up!");
		}

		// All channels need to handle errors
		tcp_channel_->onError([this](const char *message)
						  {
							  LOG_ERROR("(MyAmqpChannel) Channel error: " << message);

							  // Stop the handler. Try reconnect
							  on_error_callback_("Channel error: " + std::string(message));
						  });

		// All channels need to connect to an exchange
		auto exchange_name = channel_config_.exchange_name;
		if (!exchange_name.empty())
		{
			LOG_INFO("(MyAmqpChannel) Declaring exchange '" << exchange_name << "'");
			tcp_channel_->declareExchange(exchange_name, channel_config_.exchange_type, AMQP::durable)
						.onSuccess([this, exchange_name]()
						{
							LOG_DEBUG("(MyAmqpChannel) Exchange '" << exchange_name << "' declared successfully.");
						})
						.onError([this, exchange_name](const char *message)
						{
							LOG_WARN("(MyAmqpChannel) Exchange '" << exchange_name << "' already exists: " << message);
							if (on_error_callback_)
							{
								on_error_callback_("Exchange already exists: " + std::string(message));
							}
						});
		}
		else
		{
			// This is a fatal error - recreating the channel is not going to help - rather than continue we need to correct the configuration.
			LOG_FATAL("(MyAmqpChannel) Exchange name is empty! Not possible to declare exchange.");
			throw std::runtime_error("Exchange name is empty! Not possible to declare exchange.");
		}
	}

	const ChannelConfig channel_config_;

	// Handling the TCP Channel - not naturally thread safe so take care
	std::unique_ptr<AMQP::TcpChannel> tcp_channel_;
	std::mutex tcp_channel_mutex_;

	// Whether a channel is active or not
	std::atomic<ChannelState> channel_state_ {ChannelState::none};
	std::condition_variable channel_active_cv_;
	std::mutex channel_active_mutex_;
	std::function<void(const std::string &)> on_error_callback_;

};

class MyAmqpTxChannel : public MyAmqpChannel
{
public:
	MyAmqpTxChannel(std::unique_ptr<AMQP::TcpChannel> tcp_channel
	                , const ChannelConfig &channel_config
	                , std::function<void(const std::string &)> error_callback
	                , MyTxDataQueuePtr queue
	                , std::shared_ptr<ChannelListener> listener
	                , size_t num_transmitted=0) :
	MyAmqpChannel(channel_config
		, std::move(error_callback))
		, queue_(std::move(queue))
	, num_transmitted_(num_transmitted)
	, listener_(listener)
	{
		initialise_(std::move(tcp_channel));
	}

	~MyAmqpTxChannel()
	{
		deactivate_();
	}

	bool isActive() const
	{
		return channel_state_.load() == ChannelState::active;
	}

	void deactivate() override
	{
		deactivate_();
	}

	auto getHandler()
	{
		return queue_;
	}

	auto getNumberOfTransmittedMessages() const
	{
		return num_transmitted_.load();
	}

	void setState(ChannelState state)
	{
		channel_state_.store(state);
		listener_->onChannelStateChange(state);
	}

private:
	void initialise_(std::unique_ptr<AMQP::TcpChannel> tcp_channel)
	{
		setState(ChannelState::initialising);

		tcp_channel_ = std::move(tcp_channel);
		MyAmqpChannel::setupBaseTcpChannel();

		// We need to know when we are ready to send more data, and be able to handle acknowledgements
		tcp_channel_->confirmSelect().onSuccess([this]()
		{
			LOG_TRACE("(MyAmqpChannel) channel in a confirm mode.");
			if (channel_state_.load() == ChannelState::initialising)
			{
				LOG_DEBUG("(MyAmqpChannel) channel is now active.");
				setState(ChannelState::active);
			}
			// This occurs on the first time confirmSelect is setup
			channel_active_cv_.notify_one();
		}).onError([this](const char *message)
		{
			LOG_ERROR("(MyAmqpChannel) Error: Failed to confirm select: " << message);
			setState(ChannelState::error);
			on_error_callback_("Failed to confirm select: " + std::string(message));
		});

		// Handle sending new data whenever it is available
		transmit_thread_ = std::jthread([this]()
		{
			this->sendData_();
		});
	}

	void deactivate_()
	{
		setState(ChannelState::deactivating);
		channel_active_cv_.notify_one();
		if (transmit_thread_.joinable())
		{
			transmit_thread_.join();
		}
		setState(ChannelState::inactive);

		LOG_INFO("MyAmqpTxChannel has been deactivated");
	}

	void sendData_()
	{
		LOG_INFO("MyAmqpTxChannel data transmit for exchange " << channel_config_.exchange_name << " is starting");

		// Wait for the channel to be ready for transmission
		{
			std::unique_lock lock(channel_active_mutex_);
			channel_active_cv_.wait(lock, [this]()
			{
				return channel_state_.load() != ChannelState::initialising;
			});
		}

		if (channel_state_.load() != ChannelState::active)
		{
			LOG_DEBUG("MyAmqpTxChannel data transmit for exchange " << channel_config_.exchange_name << " never started to process data");
			return;
		}

		LOG_INFO("MyAmqpTxChannel data transmit for exchange " << channel_config_.exchange_name << " is ready to send data");

		// Wait to be notified to transmit data
		while (channel_state_.load() == ChannelState::active)
		{
			// Check if there's anything to send
			if (queue_->isEmpty())
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
				// TODO Add back in heartbeat functionality
				continue;
			}

			// Check whether we're waiting for a batch of messages to be sent
			// - transmit_ready_ - flag that we are ready to transmit again

			// Race condition here with the isTransmissionComplete - use mutex to avoid
			// If the queue is empty, popMessage is blocking!
			// LOG_INFO("Data ready for transmit");
			const auto message = queue_->peek();
			if (!message)
			{
				LOG_ERROR("(MyAmqpTxChannel) Error: Failed to get message from queue");
				continue;
			}

			// Reduce scope of mutex locking
			{
				std::lock_guard lock(tcp_channel_mutex_);
				if (!tcp_channel_->publish(channel_config_.exchange_name, channel_config_.routing_key, message->data(), message->size()))
				{
					std::ostringstream os;
					os << "Failed to publish message " << message;
					LOG_ERROR(os.str());
					setState(ChannelState::error);
					on_error_callback_(os.str());
				}
				else
				{
					LOG_TRACE("MyAmqpTxChannel: " << num_transmitted_ << " : Message " << message);
					queue_->pop(); // Only remove the message from the queue on a successful transmit
					listener_->onNumberOfTransmittedMessages(++num_transmitted_);
				}
			}
		}
		LOG_INFO("MyAmqpTxChannel data transmit for exchange " << channel_config_.exchange_name << " has stopped");
	}

private:
	std::atomic<size_t> num_transmitted_ {0};
	MyTxDataQueuePtr queue_;
	std::jthread transmit_thread_;
	std::shared_ptr<ChannelListener> listener_;
};

} // rmq
