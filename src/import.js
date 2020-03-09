module.exports = {
  sources: require('./tasks/sources'),
  stan: require('./tasks/stan'),
  stanCheck: require('./tasks/stanCheck'),
  stanClose: require('./tasks/stanClose'),
  subscriptions: require('./tasks/import/subscriptions'),
  versionTs: require('./tasks/versionTs'),
  webConnection: require('./tasks/webConnection')
}
