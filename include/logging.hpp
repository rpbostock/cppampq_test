#pragma once

#include <boost/log/trivial.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/sources/severity_channel_logger.hpp>
#include <boost/log/attributes/mutable_constant.hpp>
#include <boost/log/utility/manipulators/add_value.hpp>

BOOST_LOG_GLOBAL_LOGGER(globalLogger,
		boost::log::sources::severity_channel_logger_mt<boost::log::trivial::severity_level>);

class Logging
{
public:
	typedef boost::log::trivial::severity_level Severity;
	static void init(
			bool log_to_console = true,
			bool log_to_file = false,
			int console_log_severity = Severity::trace,
			int file_log_severity = Severity::warning,
			const std::string& log_file_name = "slate.log");
	static void disable();
	static void enable();
};

#define LOG_TRACE(ARG) do { try { BOOST_LOG_SEV(globalLogger::get(), boost::log::trivial::trace  ) << ARG; } catch (...) {} } while (false)
#define LOG_DEBUG(ARG) do { try { BOOST_LOG_SEV(globalLogger::get(), boost::log::trivial::debug  ) << ARG; } catch (...) {} } while (false)
#define LOG_INFO(ARG)  do { try { BOOST_LOG_SEV(globalLogger::get(), boost::log::trivial::info   ) << ARG; } catch (...) {} } while (false)
#define LOG_WARN(ARG)  do { try { BOOST_LOG_SEV(globalLogger::get(), boost::log::trivial::warning) << ARG; } catch (...) {} } while (false)
#define LOG_ERROR(ARG) do { try { BOOST_LOG_SEV(globalLogger::get(), boost::log::trivial::error  ) << ARG; } catch (...) {} } while (false)
#define LOG_FATAL(ARG) do { try { BOOST_LOG_SEV(globalLogger::get(), boost::log::trivial::fatal  ) << ARG; } catch (...) {} } while (false)

static void logWarn(std::string message)
{
	do { try { BOOST_LOG_SEV(globalLogger::get(), boost::log::trivial::warning) << message; } catch (...) {} } while (false);
}