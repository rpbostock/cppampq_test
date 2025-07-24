#pragma once
#include "channel_listener.hpp"
#include "channel_state.hpp"
#include "queue.hpp"
#include "logging.hpp"
#include "message_wrapper.hpp"
#include "pipe_notification_queue.hpp"

namespace rmq {

using MyTxDataQueue = PipeNotificationQueue<std::shared_ptr<std::vector<char>>>;
using MyTxDataQueuePtr = std::shared_ptr<MyTxDataQueue>;

class RxMessageWrapper : public MessageWrapper<std::shared_ptr<std::vector<char>>>
{};
using MyRxDataQueue = Queue<RxMessageWrapper>;
using MyRxAckQueue = PipeNotificationQueue<IMessageAck>;
using MyRxDataQueuePtr = std::shared_ptr<MyRxDataQueue>;
using MyRxAckQueuePtr = std::shared_ptr<MyRxAckQueue>;


/**
 * This class will handle the specific parts of AMQP relating to the channel
 * itself
 */
class MyAmqpChannel
{
public:
	MyAmqpChannel(const ChannelConfig &channel_config
	              , AMQP::TcpConnection *tcp_connection
	              , std::function<void(AMQP::TcpConnection *, const std::string &)> error_callback
	              , std::function<void()> ready_callback
	              , const std::string &channel_name) : channel_config_(channel_config)
	                                                   , on_error_callback_(std::move(error_callback))
	                                                   , ready_callback_(std::move(ready_callback))
	                                                   , channel_name_(channel_name)
	                                                   , tcp_connection_(tcp_connection)
	{}
	virtual ~MyAmqpChannel() = default;
	virtual void deactivate() = 0;


protected:
	/**
	 * This is called when constructed or after handling an error
	 */
	virtual void setupBaseTcpChannel()
	{
		tcp_channel_ = std::make_unique<AMQP::TcpChannel>(tcp_connection_);

		// All channels need to handle errors
		tcp_channel_->onError([this](const char *message)
		{
			on_error_callback_(tcp_connection_, channel_name_ + ": Channel error: " + std::string(message));
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
						.onError([this](const char *message)
						{
							if (on_error_callback_)
							{
								on_error_callback_(tcp_connection_, channel_name_ + "Exchange already exists: " + std::string(message));
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

	// Configuration of this channel - required to initialise the channel
	const ChannelConfig channel_config_;

	// Handling the TCP Channel - not naturally thread safe so only use on the event thread
	std::unique_ptr<AMQP::TcpChannel> tcp_channel_;

	// Whether a channel is active or not
	std::atomic<ChannelState> channel_state_ {ChannelState::none};

	// Common function for handling errors related to channels or connections - all need to use the same function
	std::function<void(AMQP::TcpConnection*, const std::string&)> on_error_callback_;

	// Common function for handling when a channel becomes ready - allows us to do initial processing of any queue data that may have been added before we were ready
	std::function<void()> ready_callback_;

	// Useful for identifying the channel that's acting
	const std::string channel_name_;

	// Required to pass in to the error function...
	AMQP::TcpConnection* tcp_connection_;
};

class MyAmqpRxChannel : public MyAmqpChannel
{
public:
	MyAmqpRxChannel(AMQP::TcpConnection *tcp_connection
	                , const ChannelConfig &channel_config
	                , std::function<void(AMQP::TcpConnection *, const std::string &)> error_callback
	                , std::function<void()> ready_callback
	                , const MyRxDataQueuePtr &data_queue
	                , const MyRxAckQueuePtr &ack_queue
	                , const std::shared_ptr<IChannelListener> &listener
	                , const size_t num_received = 0
	                , const size_t num_acknowledged_ = 0
	) : MyAmqpChannel(channel_config
	                  , tcp_connection
	                  , std::move(error_callback)
	                  , std::move(ready_callback)
	                  , "RX - " + channel_config.queue_name)
	    , num_received_(num_received)
	    , data_queue_(data_queue)
	    , ack_queue_(ack_queue)
	    , listener_(listener)
	    , ack_offset_due_to_reconnects_(num_acknowledged_)
	{
		initialise_();
	}

	~MyAmqpRxChannel()
	{
		deactivate_();
	}

	void deactivate() override
	{
		LOG_DEBUG(channel_name_ << ": Deactivating channel. Nothing to do here yet!");
		deactivate_();
	}

	void sendAck(size_t &current_batch_size)
	{
		if (channel_state_.load() != ChannelState::active)
		{
			return;
		}

		// Check if there's anything to send
		if (ack_queue_->isEmpty() )
		{
			return;
		}

		// If there is a problem with the channel at this point it doesn't matter so much as there's not a lot we can
		// do about it and we'd need to reprocess all received messages anyway (no way to ack messages after a disconnect)
		while (!ack_queue_->isEmpty())
		{
			const auto message = ack_queue_->peek();
			delivery_tags_.insert(message.getDeliveryTag());
			ack_queue_->pop();
		}

		// Check whether they are contiguous - they should be in our case as we aren't parallel processing messages
		auto it = std::ranges::adjacent_find(delivery_tags_,
		                                     [](uint64_t a, uint64_t b) { return b != a + 1; });

		// We could send the other individual tags, but I'm interested to see how much of an improvement this makes (if any!)
		uint64_t latest_tag = 0;
		if (it != delivery_tags_.end()) {
			// Discontinuity found: *it is the last number before the gap
			// *std::next(it) is the first number after the gap
			// We need to send an ACK for the range [*it, *std::next(it))
			LOG_DEBUG(channel_name_ << ": Discontinuity found in delivery tags - sending ACK for range " << *it << " - " << *std::next(it));
			latest_tag = *it;
		}
		else
		{
			LOG_DEBUG(channel_name_ << ": No discontinuity found in delivery tags - sending ACK for all " << delivery_tags_.size() << " messages");
			latest_tag = *delivery_tags_.rbegin();
		}

		// Get a copy of a message on the queue in case there's any issue sending it
		if (!tcp_channel_->ack(latest_tag, AMQP::multiple))
		{
			std::ostringstream os;
			os << "Failed to publish acknowledgement " << latest_tag;
			setState_(ChannelState::error);
			on_error_callback_(tcp_connection_, os.str());
		}
		else
		{
			auto num_acknowledged = ack_offset_due_to_reconnects_ + latest_tag;
			LOG_DEBUG(channel_name_  << ": ACKed - " << num_acknowledged << " : Message " << latest_tag);
			listener_->onNumberOfAcknowledgedMessages(channel_name_, num_acknowledged);
			++current_batch_size;
		}
	}

private:
	void setState_(ChannelState state)
	{
		LOG_DEBUG(channel_name_ << ": Setting state to " << static_cast<int>(state));
		channel_state_.store(state);
		listener_->onChannelStateChange(channel_name_, state);
	}

	void initialise_()
	{
		setState_(ChannelState::initialising);

		if (!listener_)
		{
			throw std::runtime_error(channel_name_ + ": Cannot create channel without a listener");
		}
		if (!data_queue_)
		{
			throw std::runtime_error(channel_name_ + ": Cannot create channel without a queue");
		}
		if (!ack_queue_)
		{
			throw std::runtime_error(channel_name_ + ": Cannot create channel without an ack queue");
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

		// Create the TCPChannel and generic bits
		MyAmqpChannel::setupBaseTcpChannel();

		// We need to know when we are ready to send more data and be able to handle acknowledgements
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
                                on_error_callback_(tcp_connection_, channel_name_ + ": Channel queue Error: " + message);
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
                            on_error_callback_(tcp_connection_, channel_name_ + ": Error binding queue to exchange: " + message);
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
                    on_error_callback_(tcp_connection_, channel_name_ + ": Error setting QoS prefetch count: " + message);
                }
            });

            tcp_channel_->consume(channel_config_.queue_name)
                        .onReceived([this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered)
                        {
                        	LOG_TRACE(channel_name_ << ": Received message with delivery tag " << deliveryTag);
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

                            data_queue_->push(std::move(wrapper));
                        	listener_->onNumberOfReceivedMessages(channel_name_, ++num_received_);

                            // Don't acknowledge at this point - we only do that once we've handled the data itself
                        })
                        .onSuccess([this]()
                        {
                            LOG_DEBUG(channel_name_ << ": Starting consuming messages");
                        	setState_(ChannelState::active);
                        	ready_callback_();
                        })
                        .onError([this](const char *message)
                        {
                            if (on_error_callback_)
                            {
                            	setState_(ChannelState::error);
                                on_error_callback_(tcp_connection_, channel_name_ + ": Error - unable to start consuming messages: " + message);
                            }
                        });
        }
	}

	void deactivate_()
	{
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
		LOG_INFO(channel_name_ << ": has been deactivated");
	}
	std::atomic<size_t> num_received_ {0};
	// std::atomic<size_t> num_acknowledged_ {0};
	MyRxDataQueuePtr data_queue_;
	MyRxAckQueuePtr ack_queue_;
	std::shared_ptr<IChannelListener> listener_;

	std::set<uint64_t> delivery_tags_; // All the tags we've received!
	std::atomic<size_t> ack_offset_due_to_reconnects_{0};
};


class MyAmqpTxChannel : public MyAmqpChannel
{
public:
	MyAmqpTxChannel(AMQP::TcpConnection *tcp_connection
	                , const ChannelConfig &channel_config
	                , std::function<void(AMQP::TcpConnection *connection, const std::string &)> error_callback
	                , std::function<void()> ready_callback
	                , MyTxDataQueuePtr queue
	                , const std::shared_ptr<IChannelListener> &listener
	                , const size_t num_transmitted = 0) : MyAmqpChannel(channel_config
	                                                                    , tcp_connection
	                                                                    , std::move(error_callback)
	                                                                    , std::move(ready_callback)
	                                                                    , "TX - " + channel_config.exchange_name)
	                                                      , num_transmitted_(num_transmitted)
	                                                      , queue_(std::move(queue))
	                                                      , listener_(listener)
	{
		initialise_();
	}

	~MyAmqpTxChannel()
	{
	}

	void deactivate() override
	{
		LOG_DEBUG(channel_name_ << ": Deactivating channel. Nothing to do here yet!");
	}

	void sendData(size_t &current_batch_size)
	{
		if (channel_state_.load() != ChannelState::active)
		{
			return;
		}

		// Check if there's anything to send
		if (queue_->isEmpty() )
		{
			return;
		}

		// Get a copy of a message on the queue in case there's any issue sending it
		const auto message = queue_->peek();
		if (!message)
		{
			LOG_ERROR(channel_name_ << ": Failed to get message from queue");
			return;
		}

		if (!tcp_channel_->publish(channel_config_.exchange_name, channel_config_.routing_key, message->data(), message->size()))
		{
			std::ostringstream os;
			os << "Failed to publish message " << message;
			setState_(ChannelState::error);
			on_error_callback_(tcp_connection_, os.str());
		}
		else
		{
			LOG_TRACE(channel_name_  << ": Transmit - " << num_transmitted_ << " : Message " << message);
			queue_->pop(); // Now remove the message from the queue as we've successfully transmitted
			listener_->onNumberOfTransmittedMessages(channel_name_, ++num_transmitted_);
			++current_batch_size;
		}
	}

private:
	void setState_(ChannelState state)
	{
		channel_state_.store(state);
		listener_->onChannelStateChange(channel_name_, state);
	}

	void initialise_()
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

		listener_->onConnect(channel_name_);
		MyAmqpChannel::setupBaseTcpChannel();

		// We need to know when we are ready to send more data and be able to handle acknowledgements
		tcp_channel_->confirmSelect().onSuccess([this]()
		{
			LOG_TRACE(channel_name_ << ": channel in a confirm mode.");
			if (channel_state_.load() == ChannelState::initialising)
			{
				LOG_DEBUG(channel_name_ << ": channel is now active.");
				setState_(ChannelState::active);
			}

			// See if there is any data to send yet!
			ready_callback_();
		}).onAck([this](uint32_t deliveryTag, bool multiple)
		{
			// We need to handle acks on the transmit side so that we know when we don't need to hold transmit data anymore
			listener_->onAcknowledgement(channel_name_, deliveryTag, multiple);
		}).onError([this](const char *message)
		{
			setState_(ChannelState::error);
			on_error_callback_(tcp_connection_, channel_name_ + ": Failed to confirm select: " + std::string(message));
		});
	}


private:
	// Total number transmitted - this is used to check whether a message has been sent that was queued. The listener
	// Can be overwritten such that the onNumberOfTransmittedMessages() allows confirmation that a message has been
	// handled
	std::atomic<size_t> num_transmitted_ {0};

	// Queue to pass data in for transmission
	MyTxDataQueuePtr queue_;

	// Receives all the event updates from this channel as the TCPChannel is not thread safe
	std::shared_ptr<IChannelListener> listener_;
};

} // rmq
