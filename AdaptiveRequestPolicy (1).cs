using RRC.Abstractions;

namespace RRC.Student;

/// <summary>
/// Адаптивная стратегия тайм-аутов и ретраев.
///
/// Идеи:
/// 1. Тайм-аут = высокий перцентиль (p99) недавних УСПЕШНЫХ ответов * запас.
///    Так стратегия сама подстраивается под любое распределение задержек и нагрузку.
/// 2. Защита от "смещения выжившего": мы видим только успешные ответы, поэтому при
///    таймаутах растёт множитель backoff (тайм-ауты увеличиваются), а при успехах он медленно спадает.
/// 3. Retry budget (token bucket, как в gRPC): ретрай стоит 1 токен, успех возвращает долю токена.
///    При перегрузке токены заканчиваются, ретраи прекращаются и сервер не добивается лавиной повторов.
/// 4. Задержка перед ретраем: экспоненциальная, с полным jitter (через переданный Random).
/// 5. На каждой следующей попытке тайм-аут чуть больше (бросать долгую попытку дорого: сервер всё равно работает).
/// </summary>
public sealed class AdaptiveRequestPolicy : IRequestPolicy
{
    // --- Параметры ---
    private const int WindowSize = 256;            // сколько последних успешных ответов помним
    private const double Quantile = 0.99;          // какой перцентиль считаем "нормальным" временем
    private const double SafetyMultiplier = 1.5;   // запас сверху над перцентилем
    private const double InitialTimeoutMs = 300;   // пока нет статистики
    private const int MinSamples = 30;             // сколько успехов нужно, прежде чем доверять статистике
    private const double MinTimeoutMs = 20;
    private const double MaxTimeoutMs = 15_000;
    private const double AttemptGrowth = 0.5;      // +50% к тайм-ауту за каждую следующую попытку
    private const int MaxAttempts = 4;

    private const double BackoffUp = 1.25;         // рост множителя при тайм-ауте
    private const double BackoffDown = 0.97;       // спад множителя при успехе
    private const double MaxBackoff = 8.0;

    private const double TokenMax = 10.0;          // retry budget
    private const double TokenPerSuccess = 0.1;

    private const double RetryDelayScale = 0.25;   // база задержки ретрая = 25% от оценки тайм-аута
    private const double MaxRetryDelayMs = 1_000;

    // --- Состояние (один экземпляр на всю симуляцию) ---
    private readonly double[] window = new double[WindowSize];
    private int windowCount;
    private int windowPos;
    private long successCount;
    private double? estimateMs;     // p99 * SafetyMultiplier
    private double backoff = 1.0;
    private double tokens = TokenMax;

    public TimeSpan GetTimeout(RequestContext context, Random random)
    {
        double baseMs = estimateMs ?? InitialTimeoutMs;
        double ms = baseMs * backoff * (1.0 + AttemptGrowth * (context.Attempt - 1));
        ms = Math.Clamp(ms, MinTimeoutMs, MaxTimeoutMs);
        return TimeSpan.FromMilliseconds(ms);
    }

    public RetryDecision OnResult(RequestObservation observation, Random random)
    {
        if (observation.Outcome == RequestOutcome.Success)
        {
            RecordSuccess(observation.Elapsed.TotalMilliseconds);
            backoff = Math.Max(1.0, backoff * BackoffDown);
            tokens = Math.Min(TokenMax, tokens + TokenPerSuccess);
            return RetryDecision.NoRetry();
        }

        // Timeout
        backoff = Math.Min(MaxBackoff, backoff * BackoffUp);

        if (observation.Attempt >= MaxAttempts || tokens < 1.0)
            return RetryDecision.NoRetry();

        tokens -= 1.0;

        double baseMs = estimateMs ?? InitialTimeoutMs;
        double cap = Math.Min(MaxRetryDelayMs,
            baseMs * RetryDelayScale * Math.Pow(2, observation.Attempt - 1) * backoff);
        double delayMs = cap * random.NextDouble(); // full jitter
        return RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(delayMs));
    }

    private void RecordSuccess(double latencyMs)
    {
        if (windowCount < WindowSize)
        {
            window[windowCount++] = latencyMs;
        }
        else
        {
            window[windowPos] = latencyMs;
            windowPos = (windowPos + 1) % WindowSize;
        }

        successCount++;
        if (successCount >= MinSamples && (estimateMs is null || successCount % 8 == 0))
        {
            var sorted = new double[windowCount];
            Array.Copy(window, sorted, windowCount);
            Array.Sort(sorted);
            int index = Math.Min(windowCount - 1, (int)Math.Ceiling(windowCount * Quantile) - 1);
            estimateMs = sorted[Math.Max(0, index)] * SafetyMultiplier;
        }
    }
}
