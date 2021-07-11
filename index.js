const nTools = require('@osmium/tools');
const moment = require('moment');
const isIPv4 = require('is-ipv4-node');
const isIPv6 = require('is-ipv6-node');
const ClickHouseConnect = require('@apla/clickhouse');

/**
 * @typedef ClickHouseOptions
 * @param {Object} [options.advanced={}]- Overload options object
 * @param {String} [options.server='localhost'] - Server address
 * @param {Boolean} [options.https=false] - Use https or http
 * @param {Number} [options.port=8123] - Server port
 * @param {String} [options.name='default'] - DB name
 * @param {String} [options.user='default'] - Username
 * @param {String} [options.password=''] - Password
 * @param {Boolean} [options.dataObjects=true] - Use object mode for rows
 * @param {Object} [options.options=false] - Query default options (for CH)
 */

/**
 * @class {ClickHouse}
 */
class ClickHouse {
	/**
	 * @constructor
	 * @param {ClickHouseOptions} options
	 * @return {ClickHouse}
	 */
	static createInstance(options) {
		return new ClickHouse(options);
	}

	/**
	 * @constructor
	 * @param {ClickHouseOptions} options
	 */
	constructor(options = {}) {
		this.options = Object.assign({
			host        : options.server ? options.server : 'localhost',
			user        : options.user ? options.user : 'default',
			password    : options.password ? options.password : '',
			path        : '/',
			port        : options.port ? options.port : 8123,
			protocol    : options.https ? 'https:' : 'http:',
			dataObjects : true,
			readonly    : false,
			queryOptions: Object.assign({
				database: options.name ? options.name : 'default'
			}, options.options)
		}, options.advanced || {});

		this.driver = new ClickHouseConnect(this.options);

		this.dbName = this.options.queryOptions.database;
		this.nullTime = '1970-01-01 00:00:00';
		this.nullDate = '1970-01-01';
	}

	/**
	 * Convert date to CH DateTime format
	 * @param {Date|Moment|Number|String} date - Source date
	 * @param {Number|String} [offset=0] - Offset in hours
	 * @param {String} [format='YYYY-MM-DD HH:mm:ss'] - Out date format
	 * @returns {String} CH DateTime
	 */
	toDateTime(date, offset = 0, format = 'YYYY-MM-DD HH:mm:ss') {
		if (typeof date === 'number' && isNaN(date) || !date) return this.nullTime;

		const res = moment.utc(date).add(offset, 'hours').format(format);
		return res === 'Invalid date' ? this.nullTime : res;
	}

	/**
	 * Convert date to CH Date format
	 * @param {Date|Moment|Number|String} date - Source date
	 * @param {Number|String} [offset=0] - Offset in hours
	 * @returns {String} CH Date
	 */
	toDate(date, offset) {
		const ret = this.toDateTime(date, offset, 'YYYY-MM-DD');
		return ret !== this.nullTime ? ret : this.nullDate;
	};

	/**
	 * Check CH Date/DateTime to null
	 * @param {Date|Moment|Number|String} date - CH Date/DateTime
	 * @param {Boolean} [convertDate=true] - Convert date
	 * @returns {number} CH Bool (UInt8 type)
	 */
	hasDate(date, convertDate = true) {
		date = convertDate ? this.toDate(date) : date;
		return date === this.nullTime || date === this.nullDate ? 0 : 1;
	}

	/**
	 * Convert boolean to CH Bool (UInt8 type)
	 * @param {Boolean} val - Value
	 * @returns {number} CH Bool (UInt8 type)
	 */
	toBool(val) {
		return val ? 1 : 0;
	}

	/**
	 * Convert ipv4 ot CH IPv4
	 * @param {String} ip - IPv4 string
	 * @returns {string|null} if not IPv4 - return null
	 */
	toIpv4(ip) {
		return isIPv4(ip) ? ip : null;
	}

	/**
	 * Convert ipv6 ot CH IPv6
	 * @param {String} ip - IPv6 string
	 * @returns {string|null} if not IPv6 - return null
	 */
	toIpv6(ip) {
		return isIPv6(ip) ? ip : null;
	}

	/**
	 * Convert float to CH Float32
	 * @param {Number|String} val - Value
	 * @returns {Number} CH Float32
	 */
	toFloat(val) {
		const float = parseFloat(val);
		return !isNaN(float) ? float : 0;
	}

	/**
	 * Convert float to CH UInt32
	 * @param {Number|String} val - Value
	 * @returns {Number} CH UInt32
	 */
	toUInt32(val) {
		const float = parseFloat(val);
		return !isNaN(float) ? float >= 0 ? float : 0 : 0;
	}

	/**
	 * Convert float to CH Int32
	 * @param {Number|String} val - Value
	 * @returns {Number} CH Int32
	 */
	toInt32(val) {
		const float = parseFloat(val);
		return !isNaN(float) ? float : 0;
	}

	/**
	 * Convert CH Date/DateTime to Date object
	 * @param {String} val - CH Date/DateTime
	 * @param {Boolean} asMoment - As moment.js object
	 */
	fromDate(val, asMoment = false) {
		if (!val || val === this.nullDate || val === this.nullTime) return null;
		const mjs = moment.utc(val, 'YYYY-MM-DD HH:mm:ss');
		return asMoment ? mjs : mjs.toDate();
	}

	/**
	 * Make error object
	 * @private
	 * @param {Error} err
	 * @returns {{code: "ERR_ASSERTION" | number | string, lineno: number, colno: number, scope: string, message: string}}
	 */
	makeErrorMessage(err) {
		return {
			message: err.toString(),
			code   : err.code,
			scope  : err.scope,
			lineno : err.lineno,
			colno  : err.colno
		};
	}

	throwError(message, val, code) {
		throw {from: 'clickhouse', message, val, code};
	}

	toInt(int, minValue, maxValue, asFloat = false) {
		if (int === true) int = 1;
		if (int === false) int = 0;
		if (int === null || int === '' || int === undefined) return '0';
		if (nTools.isNumber(int) && isNaN(int)) this.throwError('toInt::Number::isNaN', int, 21501);

		const parsedInt = asFloat ? parseFloat(int) : parseInt(int);
		if (isNaN(parsedInt)) this.throwError('toInt::String::isNaN', int, 21502);

		if (parsedInt < minValue) this.throwError('toInt::MinLength', {minValue, val: parsedInt}, 21505);
		if (parsedInt > maxValue) this.throwError('toInt::MaxLength', {maxValue, val: parsedInt}, 21506);

		return parsedInt + '';
	}

	toString(val) {
		if (val === null || val === false || val === '' || val === undefined) return '';
		if (!(nTools.isString(val) || nTools.isNumber(val))) {
			this.throwError('toString::NotStringOrNumber', val, 21510);
		}
		return val + '';
	}

	transformJSToDB(row, schema) {
		return nTools.iterate(row, (val, colName) => {
			if (!schema[colName]) this.throwError('transformJSToDB::NotInSchema', {colName, schema}, 21100);
			try {
				switch (schema[colName].type) {
					case 'UInt8':
						return this.toInt(val, 0, 255);
					case 'UInt16':
						return this.toInt(val, 0, 65535);
					case 'UInt32':
						return this.toInt(val, 0, 4294967295);
					case 'UInt64':
						return this.toInt(val, 0, Number.MAX_SAFE_INTEGER);
					case 'Int8':
						if (nTools.isBoolean(val)) {
							return this.toBool(!!val);
						}
						return this.toInt(val, -128, 127);
					case 'Int16':
						return this.toInt(val, -32768, 32767);
					case 'Int32':
						return this.toInt(val, -2147483648, 2147483647);
					case 'Int64':
						return this.toInt(val, Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER);
					case 'Float32':
						return this.toInt(val, -2147483648, 2147483647, true);
					case 'Float64':
						return this.toInt(val, Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER, true);
					case 'Date':
						return this.toDate(val);
					case 'DateTime':
						return this.toDateTime(val);
					case 'String':
						return this.toString(val);
					case 'FixedString':
						return this.toString(val);
					case 'IPv4':
						return this.toIpv4(val);
					case 'IPv6':
						return this.toIpv6(val);
					default:
						return val;
				}

			} catch (e) {
				this.throwError(`${e.message} | ${colName}`, e.val, e.code);
			}
		}, {});
	}

	transformDBToJS(row, schema) {
		return nTools.iterate(row, (val, colName) => {
			if (colName[0] === '_') return;
			if (!schema || Object.keys(schema).length === 0) return val;
			if (!schema[colName]) return val;

			switch (schema[colName].type) {
				case 'UInt8':
				case 'UInt16':
				case 'UInt32':
				case 'UInt64':
				case 'Int8':
				case 'Int16':
				case 'Int32':
				case 'Int64':
					return parseInt(val);
				case 'Float32':
				case 'Float64':
					return parseFloat(val);
				case 'Date':
				case 'DateTime':
					return this.fromDate(val);
				case 'String':
				case 'FixedString':
				case 'IPv4':
				case 'IPv6':
				default:
					return val;
			}
		}, {});
	}

	async fetchTableSchema(table) {
		const result = await this.query(`SELECT * FROM ${table} LIMIT 1`);
		return result.columns;
	}

	/**
	 * Insert slingle data row to CH
	 * @param {String} tableName - table name
	 * @param {Object|[Object]} rows - row or array of rows
	 * @param {Object} [schema=null] - table schema, if not defined, fetch automatically
	 * @param {Object} [options={}] - CH options for query
	 */
	async insert(tableName, rows, schema = null, options = {}) {
		schema = schema ? schema : await this.fetchTableSchema(tableName);

		return new Promise((resolve, reject) => {
			const _write = (row) => {
				ws.write(this.transformJSToDB(row, schema));
			};

			const ws = this.driver.query(`INSERT INTO ${tableName}`, {
				format      : 'JSONEachRow',
				queryOptions: Object.assign(this.options.queryOptions, options)
			}, (err) => {
				!err ? resolve(ws) : reject(this.makeErrorMessage(err));
			});

			if (nTools.isObject(rows)) {
				_write(rows);
			}
			if (nTools.isArray(rows)) {
				nTools.iterate(rows, row => _write(row));
			}

			ws.end();
		});
	}

	/**
	 * Optimize table
	 * @param {String} tableName - Table name
	 * @param {Boolean} [deduplicate=false] - Optimize table with DEDUPLICATE
	 * @returns {Promise<void>}
	 */
	async optimize(tableName, deduplicate = false) {
		return await this.driver.querying(`OPTIMIZE TABLE ${tableName}${deduplicate ? ' DEDUPLICATE' : ''};`);
	}

	/**
	 * @param stream
	 * @param {Boolean} onlyRows
	 * @returns {Promise<[Object]|{rows:[], info:{}, columns:[]}>}
	 * @private
	 */
	async _processQueryWS(stream, onlyRows) {
		return new Promise((resolve, reject) => {
			let columns = [];
			let info = null;
			let rows = [];

			stream.on('metadata', (meta) => columns = meta);
			stream.on('data', (row) => rows.push(row));
			stream.on('error', (err) => reject(this.makeErrorMessage(err)));
			stream.on('end', () => {
				info = stream.supplemental;
				columns = nTools.iterate(columns, (row, _, iter) => {
					iter.key(row.name);
					const type = row.type.split('(')[0];
					const param = row.type.split('(')[1] ? row.type.split('(')[1].split(')')[0] : null;
					return {
						type,
						param
					};
				}, {});

				if (rows.length > 0) {
					rows = nTools.iterate(rows, (row) => this.transformDBToJS(row, columns), []);
				}

				resolve(onlyRows ? rows : {
					columns,
					info,
					rows
				});
			});
		});
	}

	/**
	 * Make query to CH and return extended result
	 * @param {String|Array} query - query string or array of strings
	 * @param {Object} [params={}] - optional params for query
	 * @param {Boolean} [parallel=false] - parallel query call for array
	 * @returns {Promise<Object[]|[Object[]]>|null}
	 */
	async query(query, params = {}, parallel = false) {
		let rows = null;
		if (nTools.isString(query)) {
			rows = this._processQueryWS(this.driver.query(query, params));
		}

		if (nTools.isArray(query)) {
			rows = await nTools[parallel ? 'iterateParallel' : 'iterate'](query, async (row) => this._processQueryWS(this.driver.query(row, params)), []);
		}

		return rows;
	}

	/**
	 * Make query to CH and return only result's array
	 * @param {String|Array} query - query string or array of strings
	 * @param {Object} [params={}] - optional params for query
	 * @param {Boolean} [parallel=false] - parallel query call for array
	 * @returns {Promise<Object[]|[Object[]]>|null}
	 */
	async queryAll(query, params = {}, parallel = false) {
		const result = await this.query(query, params, parallel);
		return result.rows;
	}

	/**
	 * Make query to CH and return first result or null
	 * @param {String|Array} query - query string or array of strings
	 * @param {Object} [params={}] - optional params for query
	 * @param {Boolean} [parallel=false] - parallel query call for array
	 * @returns {Promise<Object>|null}
	 */
	async queryOne(query, params = {}, parallel = false) {
		const result = await this.query(query, params, parallel);
		return result.rows[0] ? result.rows[0] : null;
	}
}

module.exports = {ClickHouse};
