-- KEYS[1]: 후보별 순위를 저장하는 Sorted Set 키
-- KEYS[2]: 처리한 eventId를 저장하는 Set 키
-- ARGV[1]: eventId
-- ARGV[2]: candidateId

if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return 0
end

redis.call('ZINCRBY', KEYS[1], 1, ARGV[2])
redis.call('SADD', KEYS[2], ARGV[1])

return 1