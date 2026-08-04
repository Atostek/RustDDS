#include "ShapePublisher.hpp"
#include "ShapePubSubTypes.h"

#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/qos/PublisherQos.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>

#include <unistd.h>
#include <signal.h>
#include <string>
#include <thread>

using namespace eprosima::fastdds::dds;

ShapePublisher::ShapePublisher() : participant_(nullptr), publisher_(nullptr), topic_(nullptr), writer_(nullptr), type_(new ShapeTypePubSubType()), listener_() {}

ShapePublisher::~ShapePublisher()
{
    if (writer_ && publisher_)
    {
        publisher_->delete_datawriter(writer_);
    }

    if (publisher_ && participant_)
    {
        participant_->delete_publisher(publisher_);
    }

    if (topic_ && participant_)
    {
        participant_->delete_topic(topic_);
    }
    if (participant_)
    {
        DomainParticipantFactory::get_instance()->delete_participant(participant_);
    }
}

bool ShapePublisher::init(bool with_security)
{
    DomainParticipantQos participant_qos;
    participant_qos.name("publisher_participant");

    if (with_security)
    {
        using namespace std;
        string example_security_configuration_path = "file://../../examples/security_configuration_files/";
        string dds_sec = "dds.sec.";
        string auth = dds_sec + "auth.";
        string auth_plugin = "builtin.PKI-DH";
        string auth_prefix = auth + auth_plugin + ".";
        string access = dds_sec + "access.";
        string access_plugin = "builtin.Access-Permissions";
        string access_prefix = access + access_plugin + ".";
        string crypto = dds_sec + "crypto.";
        string crypto_plugin = "builtin.AES-GCM-GMAC";
        string plugin = "plugin";

        std::vector<pair<string, string>> security_properties = {
            pair<string, string>(auth + plugin, auth_plugin),
            pair<string, string>(access + plugin, access_plugin),
            pair<string, string>(crypto + plugin, crypto_plugin),
            pair<string, string>(auth_prefix + "identity_ca", example_security_configuration_path + "identity_ca.cert.pem"),
            pair<string, string>(auth_prefix + "identity_certificate", example_security_configuration_path + "cert.pem"),
            pair<string, string>(auth_prefix + "private_key", example_security_configuration_path + "key.pem"),
            pair<string, string>(access_prefix + "permissions_ca", example_security_configuration_path + "permissions_ca.cert.pem"),
            pair<string, string>(access_prefix + "governance", example_security_configuration_path + "governance.p7s"),
            pair<string, string>(access_prefix + "permissions", example_security_configuration_path + "permissions.p7s"),
        };

        for (pair<string, string> property : security_properties)
        {
            participant_qos.properties().properties().emplace_back(property.first, property.second);
        }
    }

    participant_ = DomainParticipantFactory::get_instance()->create_participant(0, participant_qos);

    if (participant_)
    {
        type_.register_type(participant_);
    }

    PublisherQos publisher_qos = PUBLISHER_QOS_DEFAULT;

    if (participant_)
    {
        publisher_ = participant_->create_publisher(publisher_qos);
    }

    TopicQos topic_qos = TOPIC_QOS_DEFAULT;

    if (participant_)
    {
        topic_ = participant_->create_topic("Square", type_.get_type_name(), topic_qos);
    }

    DataWriterQos datawriter_qos = DATAWRITER_QOS_DEFAULT;

    if (publisher_ && topic_)
    {
        writer_ = publisher_->create_datawriter(topic_, datawriter_qos, &listener_);
    }

    if (writer_ && topic_ && publisher_ && participant_)
    {
        std::cout << "DataWriter created for the topic Square." << std::endl;
        return true;
    }
    else
    {
        return false;
    }
}

// For handling stop signal to break the infinite loop
namespace publisher_stop
{
    volatile sig_atomic_t stop;
    void handle_interrupt(int)
    {
        stop = 1;
    }
}

void ShapePublisher::run()
{
    signal(SIGINT, publisher_stop::handle_interrupt);

    ShapeType sample;
    sample.color("BLUE");
    sample.shape_size(12);

    int number_of_messages_sent = 0;

    while (!publisher_stop::stop)
    {
        if (listener_.matched)
        {
            sample.x(10 * (number_of_messages_sent % 10));
            sample.y(10 * (number_of_messages_sent % 9));
            writer_->write(&sample);
            std::cout << "Sending sample " << number_of_messages_sent << std::endl;
            ++number_of_messages_sent;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    std::cout << "\nStopped" << std::endl;
}

void ShapePublisher::SubscriberListener::on_publication_matched(DataWriter *, const PublicationMatchedStatus &info)
{
    matched = info.current_count;
    std::cout << "Number of matched readers: " << matched << std::endl;
}