package com.netflix.astyanax.connectionpool.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lalith on 31.08.14.
 */
public class SimpleRateLimiter {
        private long lastSent;
        private double tokens;

        private double rate;
        private double rateIntervalInMillis; // in milliseconds
        private final double maxTokens;


        public SimpleRateLimiter(double initialRate, double rateIntervalInNanos, double maxTokens)
        {
            this.rate = initialRate;
            this.rateIntervalInMillis = rateIntervalInNanos * 1000000;
            this.maxTokens = maxTokens;
            this.tokens = maxTokens;
            this.lastSent = System.nanoTime();
        }

        public synchronized double tryAcquire()
        {
            double currentTokens = Math.min(maxTokens,
                                            tokens + (rate / rateIntervalInMillis * (System.nanoTime() - lastSent)));
            if (currentTokens >= 1)
            {
                tokens = currentTokens - 1;
                lastSent = System.nanoTime();
                return 0;
            }
            else
            {
                return (1 - currentTokens) * rateIntervalInMillis / rate; // Nanoseconds
            }
        }

        public synchronized double getRate()
        {
            return rate;
        }

        public synchronized void setRate(final double rate)
        {
            this.rate = rate;
        }
}
