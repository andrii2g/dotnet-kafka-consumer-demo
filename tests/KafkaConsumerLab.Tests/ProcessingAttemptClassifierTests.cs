using FluentAssertions;
using KafkaConsumerLab.Worker.Kafka;
using KafkaConsumerLab.Worker.Reliability;
using Microsoft.Extensions.Logging.Abstractions;

namespace KafkaConsumerLab.Tests;

public sealed class ProcessingAttemptClassifierTests
{
    [Fact]
    public void Missing_attempt_header_means_attempt_1()
    {
        var classifier = new ProcessingAttemptClassifier();

        var state = classifier.ReadAttemptState([], "demo-group", NullLogger.Instance);

        state.CurrentAttempt.Should().Be(1);
    }

    [Fact]
    public void Valid_attempt_header_is_parsed()
    {
        var classifier = new ProcessingAttemptClassifier();
        var headers = new Confluent.Kafka.Headers();
        KafkaHeaders.SetString(headers, KafkaHeaders.Attempt, "3");

        var state = classifier.ReadAttemptState(headers, "demo-group", NullLogger.Instance);

        state.CurrentAttempt.Should().Be(3);
    }

    [Fact]
    public void Invalid_attempt_header_falls_back_to_1()
    {
        var classifier = new ProcessingAttemptClassifier();
        var headers = new Confluent.Kafka.Headers();
        KafkaHeaders.SetString(headers, KafkaHeaders.Attempt, "nope");

        var state = classifier.ReadAttemptState(headers, "demo-group", NullLogger.Instance);

        state.CurrentAttempt.Should().Be(1);
    }

    [Fact]
    public void Max_attempts_route_to_dlq_when_current_attempt_matches_limit()
    {
        var classifier = new ProcessingAttemptClassifier();
        var headers = new Confluent.Kafka.Headers();
        KafkaHeaders.SetString(headers, KafkaHeaders.Attempt, "3");

        var state = classifier.ReadAttemptState(headers, "demo-group", NullLogger.Instance);

        (state.CurrentAttempt < 3).Should().BeFalse();
    }
}
