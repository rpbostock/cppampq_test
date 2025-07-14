#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/core.hpp>
#include <boost/log/common.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/expressions/keyword.hpp>
#include <boost/log/attributes.hpp>
#include <boost/log/utility/exception_handler.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/filter_parser.hpp>
#include <boost/log/utility/setup/formatter_parser.hpp>
#include <boost/log/utility/setup/from_stream.hpp>
#include <boost/log/utility/setup/settings.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/support/date_time.hpp>

#include "logging.hpp"

namespace blog = boost::log;

BOOST_LOG_GLOBAL_LOGGER_CTOR_ARGS(globalLogger,
               blog::sources::severity_channel_logger_mt<blog::trivial::severity_level>,
               (blog::keywords::channel = "private"));

void Logging::disable(void)
{
	boost::log::core::get()->set_logging_enabled(false);
}

void Logging::enable()
{
	boost::log::core::get()->set_logging_enabled(true);
}

void Logging::init(
		bool log_to_console,
		bool log_to_file,
		int console_log_severity,
		int file_log_severity,
		const std::string& log_file_name)
{
	using boost::posix_time::ptime;

	blog::core::get()->remove_all_sinks();

	if(log_to_file)
	{
		auto sink = blog::add_file_log(
				blog::keywords::file_name = log_file_name,
				blog::keywords::open_mode = std::ios_base::app,
				blog::keywords::rotation_size = 10 * 1024 * 1024,
				blog::keywords::auto_flush = true,
				blog::keywords::start_thread = true,
				blog::keywords::filter = boost::log::trivial::severity >= file_log_severity,
				blog::keywords::format = blog::expressions::stream
					<< blog::expressions::format_date_time< ptime >("TimeStamp", "[%Y-%m-%d %H:%M:%S.%f]")
					<< " [" << blog::expressions::attr<blog::attributes::current_thread_id::value_type>("ThreadID") << "]"
					<< " [" << blog::trivial::severity << "]"
					<< ": " << blog::expressions::smessage );
	}

	if(log_to_console)
	{
		auto sink = blog::add_console_log(std::cout,
				blog::keywords::filter = boost::log::trivial::severity >= console_log_severity,
				blog::keywords::format = blog::expressions::stream
					<< blog::expressions::format_date_time< ptime >("TimeStamp", "[%Y-%m-%d %H:%M:%S.%f]")
					<< " [" << blog::expressions::attr<blog::attributes::current_thread_id::value_type>("ThreadID") << "]"
					<< " [" << blog::trivial::severity << "]"
					<< ": " << blog::expressions::smessage );
	}

	if(log_to_console || log_to_file)
	{
		enable();
		blog::add_common_attributes();
	}
	else
	{
		disable();
	}
}
