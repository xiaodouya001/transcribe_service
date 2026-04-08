# AWS HA and Autoscaling Checklist

This document provides a production baseline for deploying Realtime Transcribe Service behind an Application Load Balancer (ALB) and Amazon ECS on AWS Fargate.

It is written for the current service behavior:

- `/health` is a liveness endpoint and returns `200` when the process is up
- `/ready` actively checks Redis and Kafka and returns `503` when a dependency is unavailable
- graceful shutdown closes active WebSocket sessions with `1001`, flushes Kafka, and then exits
- Uvicorn WebSocket keepalive is enabled with Ping/Pong

If `URL_PATH_PREFIX` is configured, prepend it to every HTTP path in this document. Example: `/transcribe-svc/health`.

This document follows the current ECS console organization used in AWS documentation as of `2026-04-08`. Console labels may drift slightly, but the grouping below is intentionally aligned to the latest ECS service and task-definition configuration dimensions rather than mixing all settings into one flat table.

---

## Scope

This checklist targets:

- single-region production deployment
- high availability across multiple Availability Zones
- elastic scaling for long-lived WebSocket traffic
- ALB in front of ECS Fargate tasks

This is a high-availability baseline, not a cross-region disaster recovery design.

---

## Quick Matrix

| AREA | Component | Field | Recommended value | Notes |
| --- | --- | --- | --- | --- |
| ECS Service | Service details | Desired tasks | `3` | Use `2` only if you run in exactly two AZs |
| ECS Service | Compute options | Capacity provider strategy | `FARGATE` as base | Add `FARGATE_SPOT` later only for burst |
| ECS Service | Deployment configuration | Availability Zone rebalancing | `Enabled` | Set explicitly even if your service is older |
| ECS Service | Deployment configuration | Health check grace period | `90 seconds` | Prevents premature replacement |
| ECS Service | Deployment configuration | Deployment type | `Rolling update` | Safe baseline |
| ECS Service | Deployment configuration | Minimum healthy percent | `100` | Keep full healthy capacity during deploy |
| ECS Service | Deployment configuration | Maximum percent | `200` | Allows one full replacement wave |
| ECS Service | Deployment configuration | Circuit breaker | `Enabled` | Stops broken rollouts |
| ECS Service | Deployment configuration | Rollback on failure | `Enabled` | Returns to last healthy revision |
| ECS Service | Networking | Subnets | `private subnets in 3 AZs` | Use at least 2 AZs |
| ECS Service | Networking | Assign public IP | `Disabled` | Put tasks behind ALB/NAT, not on public IPs |
| ECS Service | Load balancing | Target group health path | `/health` | Do not use `/ready` here |
| ECS Service | Service auto scaling | Minimum tasks | `3` | Matches HA floor |
| ECS Service | Service auto scaling | Maximum tasks | `12` | Starting cap; revisit after load tests |
| ECS Service | Service auto scaling | Policy 1 | CPU target tracking `60%` | Scale-out `60s`, scale-in `300s` |
| ECS Service | Service auto scaling | Policy 2 | Memory target tracking `75%` | Scale-out `60s`, scale-in `300s` |
| Task Definition | Task | Platform version | `LATEST` | Current Fargate runtime |
| Task Definition | Container runtime | Container stop timeout | `120 seconds` | Aligns with app graceful shutdown |
| Task Definition | Environment | `STOP_TIMEOUT` | `120` | Match container stop timeout |
| Task Definition | Environment | `WS_PING_INTERVAL` | `20` | Current code default |
| Task Definition | Environment | `WS_PING_TIMEOUT` | `10` | Current code default |
| Task Definition | Environment | `WS_MAX_CONNECTIONS` | `tested safe limit x 0.8` | Do not leave as unlimited in production |
| Target Group | Health checks | Interval | `15 seconds` | Fast enough without too much noise |
| Target Group | Health checks | Timeout | `5 seconds` | Sufficient for liveness |
| Target Group | Attributes | Deregistration delay | `120 seconds` | Match app drain window |
| Target Group | Attributes | Load balancing algorithm | `Least outstanding requests` | Better fit for uneven WebSocket load |
| ALB | Attributes | Idle timeout | `120 seconds` | Safe margin over keepalive |
| ALB | Logs | Access logs | `Enabled` | Request-level troubleshooting |
| ALB | Logs | Connection logs | `Enabled` | Useful for WebSocket troubleshooting |
| CloudWatch | Alarms | `UnHealthyHostCount` | `>= 1` | `2/2` periods, `1 min` |
| CloudWatch | Alarms | `HTTPCode_Target_5XX_Count` | `>= 5` | `3/5` periods, `1 min` |
| CloudWatch | Alarms | `/ready` synthetic check | `3/5 failures` | Keep readiness separate from ALB target health |

---

## AREA 1. ECS Service

Console path:

`Amazon ECS -> Clusters -> <cluster> -> Services -> <service> -> Update`

### Component: Service details

UI location:

`Update service -> Service details`

| Field | Recommended value | Why |
| --- | --- | --- |
| Service type | `Replica` | Standard long-running service pattern |
| Desired tasks | `3` | Maintains HA during deploys and single-task failure |
| Task definition revision | `latest approved revision` | Avoid editing service against an unvalidated revision |

Notes:

- If you only run in `2` AZs, use `Desired tasks=2`.
- If your traffic is very stable and low, you can still keep `3` tasks for operational headroom.

### Component: Compute options

UI location:

`Update service -> Compute options`

| Field | Recommended value | Why |
| --- | --- | --- |
| Launch type / capacity providers | `Capacity provider strategy` | More flexible than pinning launch type |
| Base capacity provider | `FARGATE` | Base HA capacity should not depend on Spot |
| Optional burst provider | `FARGATE_SPOT` later | Use only after baseline is stable |
| FARGATE_SPOT share | `0%` initially | Keep first production baseline simple |
| Platform version | `LATEST` | Use current Fargate runtime |

Notes:

- Start with only `FARGATE`.
- Add a small `FARGATE_SPOT` percentage later only if cost reduction matters and reconnection behavior is fully understood.

### Component: Deployment configuration

UI location:

`Update service -> Deployment configuration`

| Field | Recommended value | Why |
| --- | --- | --- |
| Deployment type | `Rolling update` | Safe baseline for production |
| Minimum healthy percent | `100` | Do not reduce below current healthy capacity |
| Maximum percent | `200` | Allows full replacement wave |
| Availability Zone rebalancing | `Enabled` | Keeps tasks spread across AZs |
| Health check grace period | `90 seconds` | Avoids premature replacement during warm-up |

### Component: Deployment failure detection

UI location:

`Update service -> Deployment configuration -> Deployment failure detection`

| Field | Recommended value | Why |
| --- | --- | --- |
| Deployment circuit breaker | `Enabled` | Stops bad rollout automatically |
| Rollback on failure | `Enabled` | Returns to last working revision |
| Deployment alarms | `Enabled` with CloudWatch alarms | Adds rollback signal from runtime symptoms |

Notes:

- `Availability Zone rebalancing` should be set explicitly even if AWS may auto-enable it for newly created services.
- `Health check grace period` defaults are often too aggressive for real dependency initialization.

### Component: Networking

UI location:

`Update service -> Networking`

| Field | Recommended value | Why |
| --- | --- | --- |
| VPC | `production VPC` | Standard production isolation |
| Subnets | `private subnets in 3 AZs` | Required for multi-AZ HA |
| Security groups | `task SG accepts only ALB SG on app port` | Limits exposure |
| Assign public IP | `Disabled` | Keep tasks private behind ALB |

Notes:

- Confirm the task security group allows outbound access to Redis, MSK, CloudWatch Logs, and Secrets Manager paths required by your environment.
- If you have only two private subnets today, add the third AZ before increasing desired tasks beyond two.

### Component: Load balancing

UI location:

`Update service -> Load balancing`

| Field | Recommended value | Why |
| --- | --- | --- |
| Load balancer type | `Application Load Balancer` | Required for HTTP/WebSocket routing |
| Container name | `<app container>` | Must match task definition |
| Container port | `8080` | Current service default |
| Health check path | `/health` | Liveness only; keeps dependency failures from draining all tasks |

Notes:

- Do not use `/ready` as the target group health path.
- `/ready` should be used for alarms or synthetic dependency monitoring instead.

### Component: Service auto scaling

Console path:

`Amazon ECS -> Clusters -> <cluster> -> Services -> <service> -> Auto scaling`

UI location:

`Service detail page -> Auto scaling tab / section`

| Field | Recommended value | Why |
| --- | --- | --- |
| Minimum tasks | `3` | Maintain HA floor |
| Maximum tasks | `12` | Reasonable starting cap |
| Policy 1 | CPU target tracking `60%` | Good first-order saturation signal |
| Policy 1 cooldowns | scale-out `60s`, scale-in `300s` | Fast out, conservative in |
| Policy 2 | Memory target tracking `75%` | Protects against gradual connection-driven memory growth |
| Policy 2 cooldowns | scale-out `60s`, scale-in `300s` | Same strategy as CPU |

Notes:

- Do not use `ALBRequestCountPerTarget` as the primary scaling metric for this service. It reflects handshake traffic more than steady-state WebSocket load.
- After your first production load-test cycle, add a custom metric for active connections per task.

---

## AREA 2. Task Definition

Console path:

`Amazon ECS -> Task definitions -> <task-definition> -> Create new revision`

### Component: Task

UI location:

`Create new revision -> Task definition configuration`

| Field | Recommended value | Why |
| --- | --- | --- |
| Launch compatibility | `Fargate` | Required deployment model |
| Operating system / architecture | `Linux / current production arch` | Keep aligned with image build |
| Task CPU / Memory | `keep current proven sizing` | Change only after load test evidence |
| Runtime platform | `same as current prod image` | Avoid unnecessary architecture drift |

Notes:

- If you do not yet have a stable baseline, keep the current task size that is already working in AWS and tune scaling before resizing tasks.

### Component: Container runtime

UI location:

`Create new revision -> Container definitions -> <container> -> Port mappings / container runtime settings`

| Field | Recommended value | Why |
| --- | --- | --- |
| Essential container | `Enabled` | Standard single-service task behavior |
| Port mapping | `8080/tcp` | Current app listener |
| Container stop timeout | `120 seconds` | Aligns with app graceful shutdown |

### Component: Environment

UI location:

`Create new revision -> Container definitions -> <container> -> Environment variables`

| Field | Recommended value | Why |
| --- | --- | --- |
| `STOP_TIMEOUT` | `120` | Matches AWS stop timeout |
| `WS_PING_INTERVAL` | `20` | Current code default |
| `WS_PING_TIMEOUT` | `10` | Current code default |
| `WS_MAX_CONNECTIONS` | `tested safe limit x 0.8` | Prevent overload and preserve headroom |

Initial fallback if you have not yet load-tested single-task connection limits:

- start with `WS_MAX_CONNECTIONS=500`
- load test one task
- adjust based on CPU, memory, Redis latency, Kafka latency, handshake failures, and WebSocket close rates

### Component: Logging

UI location:

`Create new revision -> Container definitions -> <container> -> Log collection`

| Field | Recommended value | Why |
| --- | --- | --- |
| Log driver | `awslogs` | Standard ECS/Fargate logging path |
| Log group | dedicated service log group | Easier retention and alarms |
| Stream prefix | service name | Easier task-level troubleshooting |

### Component: Secrets and config

UI location:

`Create new revision -> Container definitions -> <container> -> Environment variables / Secrets`

| Field | Recommended value | Why |
| --- | --- | --- |
| App secrets | `Secrets Manager backed` | Standard production secret handling |
| Static env vars | `plain environment variables` | Use only for non-secret values |

---

## AREA 3. Target Group

Console path:

`Amazon EC2 -> Target Groups -> <target-group>`

### Component: Health checks

UI location:

`Target group detail page -> Health checks -> Edit`

| Field | Recommended value | Why |
| --- | --- | --- |
| Protocol | `HTTP` | Standard ALB target health |
| Path | `/health` | Liveness only |
| Port | `Traffic port` | Use app listener port |
| Success codes | `200` | Simple health contract |
| Interval | `15 seconds` | Fast detection |
| Timeout | `5 seconds` | Enough for local process liveness |
| Healthy threshold | `2` | Quick return to service |
| Unhealthy threshold | `3` | Avoids flapping on brief blips |

### Component: Attributes

UI location:

`Target group detail page -> Attributes -> Edit`

| Field | Recommended value | Why |
| --- | --- | --- |
| Deregistration delay | `120 seconds` | Match app drain window |
| Load balancing algorithm | `Least outstanding requests` | Better balance for uneven long-lived connection load |

Notes:

- `/ready` should not be the target group health check path because it can turn a Redis/MSK dependency event into an ALB-wide capacity event.

---

## AREA 4. Application Load Balancer

Console path:

`Amazon EC2 -> Load Balancers -> <alb> -> Attributes -> Edit`

### Component: Attributes

UI location:

`Load balancer detail page -> Attributes -> Edit`

| Field | Recommended value | Why |
| --- | --- | --- |
| Connection idle timeout | `120 seconds` | Safe margin over app keepalive |

### Component: Logs

UI location:

`Load balancer detail page -> Attributes -> Access logs / Connection logs`

| Field | Recommended value | Why |
| --- | --- | --- |
| Access logs | `Enabled` | Request-level audit and troubleshooting |
| Connection logs | `Enabled` | Better visibility for WebSocket behavior |

Notes:

- WebSocket support rides on the same ALB listeners and rules as HTTP/HTTPS, so no special listener type is needed.

---

## AREA 5. CloudWatch and Readiness Monitoring

Console path:

`Amazon CloudWatch -> Alarms -> Create alarm`

### Component: ALB and ECS alarms

UI location:

`CloudWatch -> Alarms -> Create alarm -> Select metric`

| Metric | Recommended alarm |
| --- | --- |
| `AWS/ApplicationELB -> UnHealthyHostCount` | `Maximum >= 1` for `2/2` periods at `1 minute` |
| `AWS/ApplicationELB -> HTTPCode_Target_5XX_Count` | `Sum >= 5` for `3/5` periods at `1 minute` |
| `AWS/ECS -> CPUUtilization` | `Average >= 80` for `5 minutes` |
| `AWS/ECS -> MemoryUtilization` | `Average >= 85` for `5 minutes` |

### Component: Synthetic readiness

UI location:

`CloudWatch -> Synthetics -> Canaries -> Create canary` or your internal probe job pointing at `/ready`

| Check | Recommended alarm |
| --- | --- |
| `GET /ready` | fail `3/5` checks at `1 minute` interval |

Notes:

- The `/ready` alarm is intentionally separate from ALB target health.
- This split keeps traffic routing stable while still alerting you when Redis or Kafka becomes unhealthy.

---

## AREA 6. Custom Connection-Based Scaling

This is the next step after the baseline above is stable.

### Component: Service metric

Recommended custom metric:

- `active_ws_connections_per_task`
- or `connection_utilization = active_connections / WS_MAX_CONNECTIONS`

Recommended target:

- keep average connection utilization around `65%` to `75%`

Why:

- CPU and memory are good baseline signals
- WebSocket services are often constrained by concurrent active sessions before request rate becomes meaningful
- connection-based scaling is usually more accurate than `ALBRequestCountPerTarget` for this workload

### Component: Scale-in safety

Evaluate ECS task scale-in protection if:

- sessions are long-lived
- reconnect storms are expensive
- scale-in or deployment interruptions are user-visible

---

## Rollout Order

Apply changes in this order:

1. update ECS service deployment settings and desired count
2. align task definition stop timeout and app `STOP_TIMEOUT`
3. update target group health checks and deregistration delay
4. update ALB idle timeout and enable logs
5. enable CPU and memory autoscaling
6. create CloudWatch alarms and `/ready` synthetic checks
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
- [Amazon ECS create service console](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/create-service-console-v2.html)
- [Application Load Balancer attributes](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/edit-load-balancer-attributes.html)
- [Application Load Balancer target group attributes](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/edit-target-group-attributes.html)
- [Amazon ECS task scale-in protection](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection.html)
- [Amazon ECS Availability Zone rebalancing](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-rebalancing.html)
- [Amazon ECS Fargate capacity providers](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-capacity-providers.html)
- [Amazon Route 53 failover routing](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-policy-failover.html)
