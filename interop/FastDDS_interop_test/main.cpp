#include "ShapePublisher.hpp"
#include "ShapeSubscriber.hpp"

#include <string.h>
#include <iostream>
#include <fastdds/dds/log/FileConsumer.hpp>

int main(int number_of_arguments, char **argument_values)
{
    if (number_of_arguments != 2 || (strcmp(argument_values[1], "p") && strcmp(argument_values[1], "s") && strcmp(argument_values[1], "up") && strcmp(argument_values[1], "us")))
    {
        std::cout << "Usage: " << argument_values[0] << " p|s|up|us" << std::endl;
        std::cout << "p for publisher, s for subscriber and u for unprotected mode" << std::endl;
        return 0;
    }

    std::cout << "Start ";

    Log::SetVerbosity(Log::Kind::Info);
    Log::ClearConsumers();  // No default logging to console

    std::unique_ptr<FileConsumer> file_consumer(new FileConsumer("fastdds.log"));
    Log::ReportFilenames(true);
    Log::RegisterConsumer(std::move(file_consumer));

    if (!strcmp(argument_values[1], "p"))
    {
        std::cout << "publisher" << std::endl;

        ShapePublisher publisher;
        if (publisher.init(true))
        {
            publisher.run();
        };
    }
    else if (!strcmp(argument_values[1], "s"))
    {
        std::cout << "subscriber" << std::endl;

        ShapeSubscriber subscriber;
        if (subscriber.init(true))
        {
            subscriber.run();
        };
    }
    else if (!strcmp(argument_values[1], "up"))
    {
        std::cout << "unprotected publisher" << std::endl;

        ShapePublisher publisher;
        if (publisher.init(false))
        {
            publisher.run();
        };
    }
    else
    {
        std::cout << "unprotected subscriber" << std::endl;

        ShapeSubscriber subscriber;
        if (subscriber.init(false))
        {
            subscriber.run();
        };
    }

    return 0;
}