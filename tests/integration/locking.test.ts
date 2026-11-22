import { lockContract } from '../helpers/lock-contract.ts'
import { connectRedis, uniquePrefix } from '../helpers/redis.ts'

for (const stringNumbers of [false, true]) {
  lockContract(
    `Lock Lua (real Redis, stringNumbers=${stringNumbers})`,
    (t) => connectRedis(t, { keyPrefix: uniquePrefix('lock-contract'), stringNumbers }),
    async (redis, key) => {
      await redis.pexpire(key, 0)
    },
  )
}
