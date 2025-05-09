namespace HareQueue.RabbitMq.Context
{
    public enum ExchangeType
    {
        Direct,
        Fanout,
        Headers,
        Topic
    }

    public static class ExchangeTypeExtensions
    {
        public static string ToExchangeName(this ExchangeType type)
        {
            return type switch
            {
                ExchangeType.Direct => "direct",
                ExchangeType.Fanout => "fanout",
                ExchangeType.Headers => "headers",
                ExchangeType.Topic => "topic",
                _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
            };
        }
    }
}
