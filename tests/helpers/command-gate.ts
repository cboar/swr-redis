// Pause exactly one command at a Redis command boundary, never within a script.
export function gateCommand<Args extends unknown[], Result>(
  execute: (...args: Args) => Promise<Result>,
  timing: 'before execution' | 'after execution',
) {
  const entered = Promise.withResolvers<void>()
  const resume = Promise.withResolvers<void>()
  let intercepted = false
  return {
    entered: entered.promise,
    release: resume.resolve,
    command: async (...args: Args): Promise<Result> => {
      if (intercepted) return execute(...args)
      intercepted = true
      if (timing === 'before execution') {
        entered.resolve()
        await resume.promise
        return execute(...args)
      }
      const response = await execute(...args)
      entered.resolve()
      await resume.promise
      return response
    },
  }
}
