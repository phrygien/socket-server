docker exec -it auctav-redis redis-cli KEYS '*'
docker exec -it auctav-redis redis-cli DEL lotsState socketMeta
