#pragma once
#include <cstdint>
#include <string>
#include <amqpcpp.h>

namespace rmq
{
/**
 * @class ChannelConfig
 * @brief Represents the configuration for a communication channel.
 *
 * This class is used to define various attributes and properties
 * associated with the setup and operation of a channel used for communication.
 * It helps in managing the channel's behavior and defines its parameters.
 */
struct ChannelConfig
{
	ChannelConfig() = default;
	ChannelConfig(std::string exchange_name, std::string queue_name, std::string routing_key) :
		exchange_name(exchange_name)
		, queue_name(queue_name)
		, routing_key(routing_key)
	{}
	ChannelConfig(std::string exchange_name) :
		exchange_name(exchange_name)
	{}


	AMQP::ExchangeType exchange_type = AMQP::ExchangeType::direct;
	std::string exchange_name = "";
	std::string queue_name = "";
	std::string routing_key = "";
	size_t queue_size = 100;
	uint8_t qos_prefetch_count = 100;
	bool consume = true;
};

}
