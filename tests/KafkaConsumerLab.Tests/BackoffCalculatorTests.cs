using FluentAssertions;
using KafkaConsumerLab.Worker.Configuration;
using KafkaConsumerLab.Worker.Reliability;
using Microsoft.Extensions.Options;

namespace KafkaConsumerLab.Tests;

public sealed class BackoffCalculatorTests
{
    [Fact]
    public void Attempt_1_returns_base_delay()
    {
        var calculator = CreateCalculator(baseDelayMs: 250, maxDelayMs: 5000);

        calculator.CalculateDelayMs(1).Should().Be(250);
    }

    [Fact]
    public void Delay_grows_with_attempts()
    {
        var calculator = CreateCalculator(baseDelayMs: 250, maxDelayMs: 5000);

        calculator.CalculateDelayMs(3).Should().BeGreaterThan(calculator.CalculateDelayMs(2));
    }

    [Fact]
    public void Delay_is_capped()
    {
        var calculator = CreateCalculator(baseDelayMs: 250, maxDelayMs: 500);

        calculator.CalculateDelayMs(10).Should().Be(500);
    }

    [Fact]
    public void Invalid_attempt_is_rejected()
    {
        var calculator = CreateCalculator(baseDelayMs: 250, maxDelayMs: 5000);

        var action = () => calculator.CalculateDelayMs(0);

        action.Should().Throw<ArgumentOutOfRangeException>();
    }

    private static BackoffCalculator CreateCalculator(int baseDelayMs, int maxDelayMs) =>
        new(Options.Create(new RetryOptions
        {
            BaseDelayMs = baseDelayMs,
            MaxDelayMs = maxDelayMs
        }));
}
