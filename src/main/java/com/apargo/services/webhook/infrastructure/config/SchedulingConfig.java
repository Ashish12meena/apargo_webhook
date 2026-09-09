package com.apargo.services.webhook.infrastructure.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Scheduling is enabled in its own class rather than on the application class, so that disabling or
 * replacing it is a one-file change and the application class stays free of infrastructure concerns.
 *
 * <p>The pool is built here from {@code webhook.scheduling.pool-size} rather than left to Boot's
 * {@code spring.task.scheduling.pool.size}. Defining the bean makes Boot back off, which collapses
 * two settings that had to be kept in step into one. Spring's own default is a SINGLE thread: one
 * slow drain tick would stall the lease reclaim sweep and the relay-lag gauge along with it, so the
 * one metric that would tell you the relay is stuck stops updating precisely when the relay gets
 * stuck.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class SchedulingConfig {

    /** Named {@code taskScheduler} because that is the name {@code @Scheduled} resolves by default. */
    @Bean(name = "taskScheduler", destroyMethod = "shutdown")
    public ThreadPoolTaskScheduler taskScheduler(WebhookProperties properties) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(properties.scheduling().poolSize());
        scheduler.setThreadNamePrefix("webhook-sched-");
        // A tick that is still running at shutdown holds documents under a lease. Waiting lets it
        // finish; not waiting means the lease has to expire before anything else can claim them.
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(20);
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }
}
