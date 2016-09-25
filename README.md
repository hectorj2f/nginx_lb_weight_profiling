# nginx_lb_weight_profiling

Nginx load balacer algorithm that assigns weights to the servers based on a profiling mechanism over their workload. We used this new upstream module in ConPaaS project. With this new algo, we are able to distribute the incoming traffic accross all the available servers with better efficiency of traditional algo such as least_conn, round-robin, weight.
