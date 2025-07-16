#pragma once
#include "queue.hpp"
#include "logging.hpp"
#include "message_wrapper.hpp"

namespace rmq {

enum class ChannelState { none, initialising, active, deactivating, inactive, error };
using MyTxDataQueue = Queue<std::shared_ptr<std::vector<char>>>;
using MyTxDataQueuePtr = std::shared_ptr<MyTxDataQueue>;

class RxMessageWrapper : public MessageWrapper<std::shared_ptr<std::vector<char>>>
{};
using MyRxDataQueue = Queue<RxMessageWrapper>;
using MyRxDataQueuePtr = std::shared_ptr<Queue<RxMessageWrapper>>;

class ChannelListener
{
public:
	void onNumberOfTransmittedMessages(const std::string& channel_name, const size_t num_transmitted)
	{
		if (num_transmitted%100000 == 0)
		{
			LOG_INFO(channel_name << ": Number of transmitted messages: " << num_transmitted);
		}
		num_transmitted_ = num_transmitted;
	}

	void onChannelStateChange(const std::string& channel_name, const ChannelState state)
	{
		LOG_INFO(channel_name << "Channel state changed to " << static_cast<int>(state));
		state_.store(state);
	}

	void onRemoteQueueSize(const std::string& channel_name, const uint32_t queue_size)
	{
		remote_queue_size_.store(queue_size);
	}

	size_t getNumberOfTransmittedMessages() const
	{
		return num_transmitted_.load();
	}

	size_t getRemoteQueueSize() const
	{
		return remote_queue_size_.load();
	}

	bool isActive() const
	{
		return state_.load() == ChannelState::active;
	}
private:
	std::atomic<size_t> remote_queue_size_ {0};
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
		, std::function<void(const std::string &)> error_callback
		, const std::string& channel_name) :
		channel_config_(channel_config)
		, on_error_callback_(std::move(error_callback))
		, channel_name_(channel_name)
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
			throw std::runtime_error(channel_name_ + ": TCP channel is not set up!");
		}

		// All channels need to handle errors
		tcp_channel_->onError([this](const char *message)
						  {
							  on_error_callback_(channel_name_ + ": Channel error: " + std::string(message));
						  });

		// All channels need to connect to an exchange
		auto exchange_name = channel_config_.exchange_name;
		if (!exchange_name.empty())
		{
			LOG_INFO(channel_name_ << " Declaring exchange '" << exchange_name << "'");
			tcp_channel_->declareExchange(exchange_name, channel_config_.exchange_type, AMQP::durable)
						.onSuccess([this, exchange_name]()
						{
							LOG_DEBUG(channel_name_ << "(MyAmqpChannel) Exchange '" << exchange_name << "' declared successfully.");
						})
						.onError([this, exchange_name](const char *message)
						{
							if (on_error_callback_)
							{
								on_error_callback_(channel_name_ + "Exchange already exists: " + std::string(message));
							}
						});
		}
		else
		{
			// This is a fatal error - recreating the channel is not going to help - rather than continue we need to correct the configuration.
			std::string error_msg = channel_name_ + ": Exchange name is empty! Not possible to declare exchange.";
			LOG_FATAL(error_msg);
			throw std::runtime_error(error_msg);
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
	const std::string channel_name_;
};

class MyAmqpRxChannel : public MyAmqpChannel
{
public:
	MyAmqpRxChannel(std::unique_ptr<AMQP::TcpChannel> tcp_channel
					, const ChannelConfig &channel_config
					, std::function<void(const std::string &)> error_callback
					, const MyRxDataQueuePtr& queue
					, const std::shared_ptr<ChannelListener>& listener
					) : MyAmqpChannel(channel_config
						, std::move(error_callback)
						, "RX - " + channel_config.queue_name)
	, queue_(queue)
	, listener_(listener)
	{
		initialise_(std::move(tcp_channel));
	}

	~MyAmqpRxChannel()
	{
		deactivate_();
	}

	void deactivate() override
	{
		deactivate_();
	}

	void acknowledge(const IMessageAck& ack)
	{
		tcp_channel_->ack(ack.getDeliveryTag());
	}

private:
	void setState_(ChannelState state)
	{
		LOG_DEBUG(channel_name_ << ": Setting state to " << static_cast<int>(state));
		channel_state_.store(state);
		listener_->onChannelStateChange(channel_name_, state);
	}

	void initialise_(std::unique_ptr<AMQP::TcpChannel> tcp_channel)
	{
		setState_(ChannelState::initialising);

		if (!listener_)
		{
			throw std::runtime_error(channel_name_ + ": Cannot create channel without a listener");
		}
		if (!queue_)
		{
			throw std::runtime_error(channel_name_ + ": Cannot create channel without a queue");
		}
		if (channel_config_.exchange_name.empty())
		{
			throw std::runtime_error(channel_name_ + ": Cannot create channel with empty exchange name");
		}
		if (channel_config_.queue_name.empty())
		{
			throw std::runtime_error(channel_name_ + ": Cannot create channel with empty queue name");
		}
		if (channel_config_.routing_key.empty())
		{
			LOG_ERROR(channel_name_ << ": Expected a routing key to be provided");
		}

		tcp_channel_ = std::move(tcp_channel);
		MyAmqpChannel::setupBaseTcpChannel();

		// We need to know when we are ready to send more data, and be able to handle acknowledgements
		// The queue needs to be durable so that it survives restarts of the application
        tcp_channel_->declareQueue(channel_config_.queue_name, AMQP::durable)
                    .onSuccess
                    (
                        [this](const std::string &name,
                               uint32_t message_count,
                               uint32_t consumer_count) {
                            LOG_DEBUG(channel_name_ << ": Declared queue: '" << name <<"'");
                            listener_->onRemoteQueueSize(channel_config_.queue_name, message_count);
                        }
                    )
                    .onError([this](const char *message) {
                            if (on_error_callback_)
                            {
                            	setState_(ChannelState::error);
                                on_error_callback_(channel_name_ + ": Channel queue Error: " + message);
                            }
                        }
                    );

        LOG_INFO(channel_name_ << ": Binding queue '"<< channel_config_.queue_name << "' to exchange '" << channel_config_.exchange_name << "'");
        tcp_channel_->bindQueue(channel_config_.exchange_name, channel_config_.queue_name, channel_config_.routing_key)
                    .onSuccess([this]() {
                        LOG_DEBUG(channel_name_ << ": Bound queue to exchange.");
                    })
                    .onError([this](const char *message) {
                        if (on_error_callback_)
                        {
                            setState_(ChannelState::error);
                            on_error_callback_(channel_name_ + ": Error binding queue to exchange: " + message);
                        }
                    });

        if (channel_config_.consume)
        {
            // Channel Prefetch Count
            // Set a limit of the number of unacked messages permitted on a channel.
            // Once the number reaches the configured count, RabbitMQ will stop delivering more messages on the channel
            // unless at least one of the outstanding ones is acknowledged.
            // False - don't share counter between all consumers on the same channel; we have one consumer per channel anyway.
            tcp_channel_->setQos(channel_config_.qos_prefetch_count, false)
            .onSuccess([this]()
            {
              LOG_DEBUG(channel_name_ << ": Set QoS prefetch count to " << channel_config_.qos_prefetch_count);
            })
            .onError([this](const char *message)
            {
                if (on_error_callback_)
                {
                	setState_(ChannelState::error);
                    on_error_callback_(channel_name_ + ": Error setting QoS prefetch count: " + message);
                }
            });

            tcp_channel_->consume(channel_config_.queue_name)
                        .onReceived([this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered)
                        {
                        	LOG_DEBUG(channel_name_ << ": Received message with delivery tag " << deliveryTag);
                        	RxMessageWrapper wrapper;
                            wrapper.getMessage() = std::make_shared<std::vector<char>>(message.bodySize());
	                        std::copy_n(message.body(), message.bodySize(),
	                                  wrapper.getMessage()->data());

	                        IMessageAck ack;
                            ack.setDeliveryTag(deliveryTag);
                            ack.setRedelivered(redelivered);
                            ack.setExchange(channel_config_.exchange_name);
                            ack.setRoutingKey(channel_config_.routing_key);
                            wrapper.setAck(std::move(ack));

                            queue_->push(std::move(wrapper));

                            // Don't acknowledge at this point - we only do that once we've handled the data itself
                        })
                        .onSuccess([this]()
                        {
                            LOG_DEBUG(channel_name_ << ": Starting consuming messages");
                        	setState_(ChannelState::active);
                        })
                        .onError([this](const char *message)
                        {
                            if (on_error_callback_)
                            {
                            	setState_(ChannelState::error);
                                on_error_callback_(channel_name_ + ": Error - unable to start consuming messages: " + message);
                            }
                        });
        }
	}




	void deactivate_()
	{
		// TODO Need to update this
		setState_(ChannelState::deactivating);
		tcp_channel_->cancel(channel_config_.queue_name).onSuccess([this]()
		{
			setState_(ChannelState::inactive);
		}).onError([this](const char *message)
		{
			LOG_ERROR(channel_name_ << ": Error - unable to cancel consuming messages: " << message);
			setState_(ChannelState::error);
		}
		);

		auto start = std::chrono::steady_clock::now();
		while (channel_state_.load() == ChannelState::deactivating && std::chrono::steady_clock::now() - start < std::chrono::milliseconds(100))
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		if (channel_state_.load() == ChannelState::deactivating)
		{
			setState_(ChannelState::inactive);
		}
		LOG_INFO(channel_name_ << ":MyAmqpTxChannel has been deactivated");
	}

	std::atomic<size_t> num_transmitted_ {0};
	MyRxDataQueuePtr queue_;
	std::shared_ptr<ChannelListener> listener_;
};

class MyAmqpTxChannel : public MyAmqpChannel
{
public:
	MyAmqpTxChannel(std::unique_ptr<AMQP::TcpChannel> tcp_channel
	                , const ChannelConfig &channel_config
	                , std::function<void(const std::string &)> error_callback
	                , MyTxDataQueuePtr queue
	                , const std::shared_ptr<ChannelListener>& listener
	                , const size_t num_transmitted=0) : MyAmqpChannel(channel_config
	                                                                  , std::move(error_callback)
	                                                                  , "TX - " + channel_config.exchange_name)
	                                                    , num_transmitted_(num_transmitted)
	                                                    , queue_(std::move(queue))
	                                                    , listener_(listener)
	{
		initialise_(std::move(tcp_channel));
	}

	~MyAmqpTxChannel()
	{
		deactivate_();
	}

	void deactivate() override
	{
		deactivate_();
	}


private:
	void setState_(ChannelState state)
	{
		channel_state_.store(state);
		listener_->onChannelStateChange(channel_name_, state);
	}

	void initialise_(std::unique_ptr<AMQP::TcpChannel> tcp_channel)
	{
		setState_(ChannelState::initialising);

		if (channel_config_.exchange_name.empty())
		{
			throw std::runtime_error(channel_name_ + ": Cannot create channel with empty exchange name");
		}
		if (!queue_)
		{
			throw std::runtime_error(channel_name_ + ": Cannot create channel without a queue");
		}
		if (!listener_)
		{
			throw std::runtime_error(channel_name_ + ": Cannot create channel without a listener");
		}

		tcp_channel_ = std::move(tcp_channel);
		MyAmqpChannel::setupBaseTcpChannel();

		// We need to know when we are ready to send more data, and be able to handle acknowledgements
		tcp_channel_->confirmSelect().onSuccess([this]()
		{
			LOG_TRACE(channel_name_ << ": channel in a confirm mode.");
			if (channel_state_.load() == ChannelState::initialising)
			{
				LOG_DEBUG(channel_name_ << ": channel is now active.");
				setState_(ChannelState::active);
			}
			// This occurs on the first time confirmSelect is setup
			channel_active_cv_.notify_one();
		}).onError([this](const char *message)
		{
			setState_(ChannelState::error);
			on_error_callback_(channel_name_ + ": Failed to confirm select: " + std::string(message));
		});

		// Handle sending new data whenever it is available
		transmit_thread_ = std::jthread([this]()
		{
			this->sendData_();
		});
	}

	void deactivate_()
	{
		setState_(ChannelState::deactivating);
		channel_active_cv_.notify_one();
		if (transmit_thread_.joinable())
		{
			transmit_thread_.join();
		}
		setState_(ChannelState::inactive);

		LOG_INFO(channel_name_ << ": has been deactivated");
	}

	void sendData_()
	{
		LOG_INFO(channel_name_ << ": starting data transmit");

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
			LOG_DEBUG(channel_name_ << ": data transmit never started to process data");
			return;
		}

		LOG_INFO(channel_name_ << ": data transmit is ready to send data");

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
				LOG_ERROR(channel_name_ << ": Failed to get message from queue");
				continue;
			}

			// Reduce scope of mutex locking
			{
				std::lock_guard lock(tcp_channel_mutex_);
				if (!tcp_channel_->publish(channel_config_.exchange_name, channel_config_.routing_key, message->data(), message->size()))
				{
					std::ostringstream os;
					os << "Failed to publish message " << message;
					setState_(ChannelState::error);
					on_error_callback_(os.str());
				}
				else
				{
					LOG_TRACE(channel_name_  << ": Transmit - " << num_transmitted_ << " : Message " << message);
					queue_->pop(); // Only remove the message from the queue on a successful transmit
					listener_->onNumberOfTransmittedMessages(channel_config_.exchange_name, ++num_transmitted_);
				}
			}
		}
		LOG_INFO(channel_name_ << ": Data transmit has stopped");
	}

private:
	std::atomic<size_t> num_transmitted_ {0};
	MyTxDataQueuePtr queue_;
	std::jthread transmit_thread_;
	std::shared_ptr<ChannelListener> listener_;
};

} // rmq
