const web = require('moleculer-web')
const trino = require('presto-client')
const JSONbig = require('json-bigint')
const {DateTime} = require('luxon')

module.exports = {
  name: 'trino',
  mixins: [web],

  settings: {
    trino: {
      host: process.env.TRINO_HOST || 'trino.trino-example',
      catalog: process.env.TRINO_CATALOG || 'cockroachdb',
      schema: process.env.TRINO_SCHEMA || 'public',
      user: 'trino',
      engine: 'trino',
      jsonParser: JSONbig,
    },

    routes: [
      {
        path: '/api',

        whitelist: [
          '**',
        ],

        use: [],
        authentication: false,
        authorization: false,
        mergeParams: true,
        autoAliases: true,
        aliases: {},
        mappingPolicy: 'all',
        callingOptions: {},
        logging: true,

        bodyParsers: {
          json: {
            strict: false,
            limit: '1MB',
          },
          urlencoded: {
            extended: true,
            limit: '1MB',
          },
        },
      },
    ],

    assets: {
      path: '/admin',
      folder: 'public/admin',
    },

    port: process.env.PORT || 3000,
  },

  actions: {
    execute: {
      rest: 'GET /:view',
      async handler(ctx) {
        const query = `SELECT * FROM ${ctx.params.view}`
        this.logger.info(query)
        const result = await this.execute(query)
        const json = result.data.map((row) => {
          const obj = {}
          row.forEach((col, i) => obj[result.columns[i].name] = col)
          obj._refresh =  DateTime.now().toFormat('MM/dd/yy')
          return obj
        })
        return json
      },
    },
  },

  methods: {
    execute(query) {
      return new this.Promise((resolve, reject) => {
        this.trino.execute({
          query,
          data: function (error, data, columns, stats) {
            resolve({data, columns})
          },
          success: function (error, stats) {
          },
          error: function (error) {
            reject(error)
          },
        })
      })
    },
  },

  created() {
    this.trino = new trino.Client({...this.settings.trino, source: this.broker.nodeID})
  },

}
