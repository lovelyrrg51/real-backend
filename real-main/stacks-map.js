// https://github.com/dougmoscrop/serverless-plugin-split-stacks#advanced-usage
// https://github.com/sid88in/serverless-appsync-plugin#split-stacks-plugin
module.exports = (resource, logicalId) => {
  const defaults = { allowSuffix: true, force: true }
  if (resource.Type == 'AWS::AppSync::FunctionConfiguration') {
    return Object.assign(defaults, { destination: 'AppSyncFunctionConfiguration' })
  }
  if (resource.Type == 'AWS::AppSync::Resolver') return Object.assign(defaults, { destination: 'AppSyncResolver' })
}
