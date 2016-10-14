# v5

 - *Important*. ClusterClient and Ring now chose random node/shard when command does not have any keys or command info is not fully available. Also clients use EVAL and EVALSHA keys to pick the right node.
 - Tx is refactored using Pipeline API so it implements Cmdable interface.
