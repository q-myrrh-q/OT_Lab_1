using RRC.Abstractions;

namespace RRC.Student;

/// <summary>
/// Адаптивная стратегия тайм-аутов и ретраев.
///
/// Идея:
///  1. Тайм-аут берётся не константой, а из недавней статистики: квантиль (95%) длительностей
///     последних попыток, умноженный на запас. Тайм-ауты в статистике учитываются как "минимум
///     в 3 раза больше текущего тайм-аута" (цензурированные данные) - поэтому при росте нагрузки
///     оценка автоматически растёт, и стратегия не устраивает лавину ретраев.
///  2. Тайм-аут растёт с номером попытки: если запрос уже один раз не уложился, вероятно,
///     сервер перегружен, и ждать дольше выгоднее, чем плодить новые попытки.
///  3. Ретраи ограничены: не более MaxAttempts попыток на запрос и общий "бюджет ретраев"
///     (token bucket) - каждый новый запрос пополняет бюджет, каждый ретрай тратит. Так число
///     попыток на запрос не превысит заданную долю даже при массовых тайм-аутах.
///  4. Пауза перед ретраем - экспоненциальная с полным джиттером (через переданный Random,
///     чтобы результат воспроизводился при одном seed).
/// </summary>
public sealed class AdaptiveRequestPolicy : IRequestPolicy
{
    private const int MaxAttempts = 4;
    private const int WindowSize = 200;
    private const int MinSamples = 20;

    private const double Quantile = 0.95;
    private const double Margin = 1.4;
    private const double CensoredFactor = 3.0;
    private const double AttemptGrowth = 1.6;

    private const double InitialTimeoutMs = 200;
    private const double MinTimeoutMs = 20;
    private const double MaxTimeoutMs = 10000;

    private const double RetryTokensPerRequest = 0.15;
    private const double RetryTokensCap = 30;

    private readonly double[] window = new double[WindowSize];
    private readonly double[] scratch = new double[WindowSize];
    private int count;
    private int next;
    private double estimateMs = InitialTimeoutMs;
    private double retryTokens = 10;

    public TimeSpan GetTimeout(RequestContext context, Random random)
    {
        if (context.Attempt == 1)
            retryTokens = Math.Min(RetryTokensCap, retryTokens + RetryTokensPerRequest);

        double ms = estimateMs * Math.Pow(AttemptGrowth, context.Attempt - 1);
        ms = Math.Clamp(ms, MinTimeoutMs, MaxTimeoutMs);
        return TimeSpan.FromMilliseconds(ms);
    }

    public RetryDecision OnResult(RequestObservation observation, Random random)
    {
        double elapsedMs = observation.Elapsed.TotalMilliseconds;

        if (observation.Outcome == RequestOutcome.Success)
        {
            Record(elapsedMs);
            return RetryDecision.NoRetry();
        }

        // Тайм-аут: реальная длительность неизвестна, но не меньше elapsed.
        Record(elapsedMs * CensoredFactor);

        if (observation.Attempt >= MaxAttempts || retryTokens < 1)
            return RetryDecision.NoRetry();

        retryTokens -= 1;

        double ceiling = Math.Min(estimateMs * 0.5 * Math.Pow(2, observation.Attempt - 1), 1000);
        double delayMs = random.NextDouble() * ceiling;
        return RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(delayMs));
    }

    private void Record(double ms)
    {
        window[next] = ms;
        next = (next + 1) % WindowSize;
        if (count < WindowSize) count++;
        if (count < MinSamples) return;

        Array.Copy(window, scratch, count);
        Array.Sort(scratch, 0, count);
        int index = Math.Min(count - 1, (int)Math.Ceiling(count * Quantile) - 1);
        estimateMs = Math.Clamp(scratch[index] * Margin, MinTimeoutMs, MaxTimeoutMs);
    }
}
