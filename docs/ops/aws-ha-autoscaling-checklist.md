# AWS HA and Autoscaling Checklist

This document provides a production baseline for deploying Realtime Transcribe Service behind an Application Load Balancer (ALB) and Amazon ECS on AWS Fargate.

It is written for the current service behavior:

- `/health` is a liveness endpoint and returns `200` when the process is up
- `/ready` actively checks Redis and Kafka and returns `503` when a dependency is unavailable
- graceful shutdown closes active WebSocket sessions with `1001`, flushes Kafka, and then exits
- Uvicorn WebSocket keepalive is enabled with Ping/Pong

If `URL_PATH_PREFIX` is configured, prepend it to every HTTP path in this document. Example: `/transcribe-svc/health`.

---

## Scope

This checklist targets:

- single-region production deployment
- high availability across multiple Availability Zones
- elastic scaling for long-lived WebSocket traffic
- ALB in front of ECS Fargate tasks

This is a high-availability baseline, not a cross-region disaster recovery design.

---

## Recommended Baseline

| Area | Field | Recommended value | Why |
| --- | --- | --- | --- |
| ECS Service | Desired tasks | `3` | Keeps capacity across AZs during deployments and single-task failure |
| ECS Service | Minimum tasks for autoscaling | `3` | Prevents scaling below HA baseline |
| ECS Service | Maximum tasks for autoscaling | `12` | Starting cap; tune after load tests |
| ECS Service | Availability Zone rebalancing | `Enabled` | Keeps tasks spread across AZs |
| ECS Service | Health check grace period | `90 seconds` | Avoids early replacement during task warm-up |
| ECS Service | Deployment type | `Rolling update` | Standard controlled rollout |
| ECS Service | Minimum healthy percent | `100` | Do not drop below current healthy capacity during deploy |
| ECS Service | Maximum percent | `200` | Allows full replacement wave during rolling deploy |
| ECS Service | Deployment circuit breaker | `Enabled` | Stops broken deployments early |
| ECS Service | Rollback on failure | `Enabled` | Returns to last healthy revision automatically |
| Task Definition | Platform version | `LATEST` | Use current Fargate runtime |
| Task Definition | Container stop timeout | `120 seconds` | Aligns AWS SIGTERM budget with app shutdown budget |
| App env | `STOP_TIMEOUT` | `120` | Matches container stop timeout |
| App env | `WS_PING_INTERVAL` | `20` | Current code default |
| App env | `WS_PING_TIMEOUT` | `10` | Current code default |
| App env | `WS_MAX_CONNECTIONS` | `single-task tested safe limit x 0.8` | Avoids overload and gives scaling headroom |
| Target Group | Health check path | `/health` | Prevents Redis/Kafka blips from removing all targets |
| Target Group | Health check port | `Traffic port` | Standard ALB health check |
| Target Group | Success codes | `200` | Simple liveness contract |
| Target Group | Timeout | `5 seconds` | Enough for local process liveness |
| Target Group | Interval | `15 seconds` | Fast enough detection without excess noise |
| Target Group | Healthy threshold | `2` | Quick recovery |
| Target Group | Unhealthy threshold | `3` | Avoids overreaction to transient failures |
| Target Group | Deregistration delay | `120 seconds` | Matches app shutdown window and connection draining |
| Target Group | Load balancing algorithm | `Least outstanding requests` | Better default for uneven long-lived connection load |
| ALB | Idle timeout | `120 seconds` | Safe margin for WebSocket keepalive traffic |
| ALB | Access logs | `Enabled` | Request-level troubleshooting |
| ALB | Connection logs | `Enabled` | Useful for WebSocket and connection troubleshooting |

If you only run in two Availability Zones, use `Desired tasks=2` and `Minimum tasks=2`. Everything else can stay the same.

---

## Console Checklist

### 1. ECS Service

Open:

`Amazon ECS -> Clusters -> <cluster> -> Services -> <service> -> Update`

Set:

| Field | Value |
| --- | --- |
| Desired tasks | `3` |
| Availability Zone rebalancing | `Enabled` |
| Health check grace period | `90 seconds` |
| Deployment type | `Rolling update` |
| Minimum healthy percent | `100` |
| Maximum percent | `200` |
| Deployment circuit breaker | `Enabled` |
| Rollback on failure | `Enabled` |

### 2. ECS Service Auto Scaling

Open:

`Amazon ECS -> Clusters -> <cluster> -> Services -> <service> -> Auto scaling`

Set:

| Field | Value |
| --- | --- |
| Minimum tasks | `3` |
| Maximum tasks | `12` |
| Policy 1 | `Target tracking`, metric=`ECSServiceAverageCPUUtilization`, target=`60`, scale-out cooldown=`60s`, scale-in cooldown=`300s` |
| Policy 2 | `Target tracking`, metric=`ECSServiceAverageMemoryUtilization`, target=`75`, scale-out cooldown=`60s`, scale-in cooldown=`300s` |

Do not use `ALBRequestCountPerTarget` as the primary scaling metric for this service. It is handshake-heavy and usually does not reflect steady-state WebSocket load.

### 3. Task Definition

Open:

`Amazon ECS -> Task definitions -> <task-definition> -> Create new revision`

Set:

| Field | Value |
| --- | --- |
| Platform version | `LATEST` |
| Container stop timeout | `120 seconds` |
| Env `STOP_TIMEOUT` | `120` |
| Env `WS_PING_INTERVAL` | `20` |
| Env `WS_PING_TIMEOUT` | `10` |
| Env `WS_MAX_CONNECTIONS` | `tested safe connection ceiling x 0.8` |

Initial fallback if you have not run a connection limit test yet:

- start with `WS_MAX_CONNECTIONS=500`
- load test one task
- lower or raise after observing CPU, memory, Kafka latency, Redis latency, and close/error rates

### 4. ALB Target Group

Open:

`Amazon EC2 -> Target Groups -> <target-group> -> Health checks -> Edit`

Set:

| Field | Value |
| --- | --- |
| Protocol | `HTTP` |
| Path | `/health` |
| Port | `Traffic port` |
| Success codes | `200` |
| Timeout | `5 seconds` |
| Interval | `15 seconds` |
| Healthy threshold | `2` |
| Unhealthy threshold | `3` |

Then open:

`Amazon EC2 -> Target Groups -> <target-group> -> Attributes -> Edit`

Set:

| Field | Value |
| --- | --- |
| Deregistration delay | `120 seconds` |
| Load balancing algorithm | `Least outstanding requests` |

Use `/ready` for alarms or synthetic checks, not as the target group health path. `/ready` failing means a dependency is degraded; it should not automatically cause every task to be removed from the ALB.

### 5. ALB

Open:

`Amazon EC2 -> Load Balancers -> <alb> -> Attributes -> Edit`

Set:

| Field | Value |
| --- | --- |
| Connection idle timeout | `120 seconds` |
| Access logs | `Enabled` |
| Connection logs | `Enabled` |

---

## Alarm Baseline

Open:

`Amazon CloudWatch -> Alarms -> Create alarm`

Create at least these alarms:

| Namespace / Metric | Suggested alarm |
| --- | --- |
| `AWS/ApplicationELB -> UnHealthyHostCount` | `Maximum >= 1` for `2/2` periods at `1 minute` |
| `AWS/ApplicationELB -> HTTPCode_Target_5XX_Count` | `Sum >= 5` for `3/5` periods at `1 minute` |
| `AWS/ECS -> CPUUtilization` | `Average >= 80` for `5 minutes` |
| `AWS/ECS -> MemoryUtilization` | `Average >= 85` for `5 minutes` |
| Synthetic `GET /ready` | fail `3/5` checks at `1 minute` interval |

The `/ready` alarm is important because the ALB health check intentionally uses `/health`.

---

## Scaling Strategy Notes

### What to use first

Start with:

- CPU target tracking
- Memory target tracking
- static `WS_MAX_CONNECTIONS` guardrail in the service

### What to add next

Add a custom CloudWatch metric after the first production load-test cycle:

- `active_ws_connections_per_task`
- or `connection_utilization = active_connections / WS_MAX_CONNECTIONS`

Recommended target:

- keep average connection utilization around `65%` to `75%`

This is usually more accurate than request count for WebSocket workloads.

### Scale-in safety

If sessions are long-lived and reconnection cost matters, evaluate ECS task scale-in protection so tasks with active sessions are less likely to be terminated first during scale-in or rolling deployment.

---

## Rollout Order

Apply changes in this order:

1. set ECS desired count and deployment settings
2. align task definition stop timeout and app `STOP_TIMEOUT`
3. update target group health check and deregistration delay
4. update ALB idle timeout and enable logs
5. enable autoscaling policies
6. create alarms
7. run a deployment test and a scale-out / scale-in game day

During the game day, explicitly verify:

- new connections keep succeeding during rolling deployment
- existing WebSocket sessions receive graceful close behavior during task termination
- Kafka flush completes before task exit
- no large spike in `5XX`, reconnect failures, or Redis ownership conflicts

---

## Future Enhancements

Consider these only after the baseline above is stable:

- scheduled scaling for predictable peak windows
- custom connection-based autoscaling
- small `FARGATE_SPOT` share for burst capacity only, not base capacity
- second-region failover with Route 53 or Global Accelerator if business requirements include region-level disaster recovery

---

## AWS References

- [Amazon ECS service auto scaling](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-auto-scaling.html)
- [Amazon ECS target tracking scaling policies](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-autoscaling-targettracking.html)
- [Application Auto Scaling target tracking overview](https://docs.aws.amazon.com/autoscaling/application/userguide/target-tracking-scaling-policy-overview.html)
- [Amazon ECS deployment circuit breaker](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-circuit-breaker.html)
- [Amazon ECS deployment alarms](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-alarm-failure.html)
- [Amazon ECS service definition parameters](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service_definition_parameters.html)
- [Application Load Balancer attributes](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/edit-load-balancer-attributes.html)
- [Application Load Balancer target group attributes](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/edit-target-group-attributes.html)
- [Amazon ECS task scale-in protection](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection.html)
- [Amazon ECS Availability Zone rebalancing](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-rebalancing.html)
- [Amazon ECS Fargate capacity providers](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-capacity-providers.html)
- [Amazon Route 53 failover routing](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-policy-failover.html)
