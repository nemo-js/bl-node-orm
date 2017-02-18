var connection;

var _entities = [];

class Query {
  constructor(entity) {
    this.entity = entity;
    this.filter = {};
    this.limit = null;
    this.toSkip = null;
    this.imports = [];
  }

  include(assoc) {
    this.imports.push(assoc);
    return this;
  }

  where(whereClause) {
    for (let prop in whereClause) {
      this.filter[prop] = whereClause[prop];
    }
    return this;
  }

  findOne(whereClause) {
    this.where(whereClause);
    this.limit = 1;
    return this;
  }

  getAll() {
    return this;
  }

  skip(count) {
    this.toSkip = count;
    return this;
  }

  take(count) {
    this.limit = count;
    return this;
  }

  __buildQuery() {
    var condition = BLOrm.compileWhere(this.entity, this.filter);
    var where = "";
    if (condition != null && condition != "" && condition.trim() != "") {
      where = "WHERE " + condition;
    }

    var limit = this.limit == null ? "" : "LIMIT " + this.limit;
    var skip = this.limit == null || this.toSkip == null ? "" : " OFFSET  " + this.toSkip;

    var joins = "";
    var columns = BLOrm.getColumns(this.entity);
    for (let i = 0; i < this.imports.length; i++) {
      const include = this.imports[i];
      const propertyToImport = this.entity.properties[include];
      if (propertyToImport == null) continue;
      var otherType = propertyToImport.type; //todo: if .type is string dynamically get entity
      const extraImports = BLOrm.getColumns(otherType, include, include + "_");
      columns += ", " + extraImports;
      //todo: dont take id karfwtra
      joins += `\n LEFT JOIN ${otherType.tableName} as ${include} on ${include}.id = ${this.entity.tableName}.${propertyToImport.fk}`;
    }

    return `SELECT ${columns} FROM ${this.entity.tableName} ${joins} ${where} ${limit}${skip}`;
  }

  exec() {
    return BLOrm._runInContext((conenction, done) => {
      connection.query(BLOrm._logQuery(this.__buildQuery()), (error, results, fields) => {
        if (error == null) {
          results = BLOrm.fixResults(this.entity, results);
        }
        if (this.limit == 1 && results != null) {
          results = results[0];
        }
        done(error, results);
      });
    });
  }
}

class Transaction {

  constructor() {
    this.actions = [];
    this.results = [];
  }

  insert(entity, instance) {
    this.actions.push({
      entity: entity,
      instance: instance,
      type: "insert"
    });
    return this;
  }

  commit() {
    return BLOrm._runInContext((conenction, done) => {
      connection.beginTransaction((err) => {
        this.__executeNext(connection, done);
      });
    });
  }

  __executeNext(connection, done) {
    if (this.actions.length == 0) {
      this.__alFinished(connection, done);
      return;
    }

    var action = this.actions[0];

    var sql = "";
    switch (action.type) {
      case "insert":
        sql = BLOrm.getInsertStatement(action.entity, action.instance);
        break;
    }

    connection.query(BLOrm._logQuery(sql), (error, results, fields) => {
      if (error) {
        return connection.rollback(() => {
          done(error);
        });
      }

      this.results.push(results);

      this.actions.splice(0, 1);
      this.__executeNext(connection, done);
    });

  }

  __alFinished(connection, done) {
    connection.commit((commitErr) => {
      if (commitErr) {
        return connection.rollback(() => {
          done(commitErr);
        });
      }

      done(null, this.results);
    });
  }

}

class BLOrm {
  static _development() {
    return true;
  }

  static init(conn) {
    connection = conn;
  }

  static registerEntities(entities) {
    _entities = entities;
  }

  static _logQuery(code) {
    if (BLOrm._development() === false) return code;

    console.log("BL-ORM QUERY: " + code);
    return code;
  }

  static _runInContext(worker) {
    return new Promise((resolve, reject) => {
      worker(connection, (error, results) => {
        if (error) throw error;
        resolve(results);
      });
    });
  }

  static _sqlizeValue(prop, value) {
    if (prop.type === "string") {
      return `'${value}'`;
    }

    return value;
  }

  static getColumns(entity, alias, colAliasPrefix) {
    if (alias == null || alias == "") {
      alias = entity.tableName;
    }

    var cols = "";
    for (let propName in entity.properties) {
      if (cols != "") {
        cols += ", ";
      }

      var prop = entity.properties[propName];
      if (prop.fk != null) {
        propName = prop.fk;
      }
      cols += alias + "." + propName;

      if (colAliasPrefix != null && colAliasPrefix != "") {
        cols += " as " + colAliasPrefix + propName;
      }
    }

    return cols;
  }

  static compileWhere(entity, whereCondition) {
    var code = "";

    for (var field in whereCondition) {
      if (!whereCondition.hasOwnProperty(field)) {
        continue;
      }

      const value = whereCondition[field];
      const prop = entity.properties[field];

      if (prop == null) {
        throw new Error(`Entity '${entity.name}' does not have a property named '${field}'`);
      }

      const columnName = BLOrm.getPropertyName(field, prop);

      if (typeof value === "string" || typeof value === "number") {
        var sqlpizedValue = BLOrm._sqlizeValue(prop, value);
        code += `${columnName} = ${sqlpizedValue} `;
      }
      else {
        throw new Error(`Type '${typeof value}' is not supported by BLOrm for where condition`);
      }
    }

    return code.trim();
  }

  static fixResults(entity, results) {
    if (results == null) return [];

    var correctResults = [];

    for (let i = 0; i < results.length; i++) {
      var result = results[i];
      correctResults.push(BLOrm.fixResult(entity, result, ""));
    }

    return correctResults;
  }

  static fixResult(entity, result, colPrefix) {
    if (colPrefix == null) colPrefix = "";
    //todo: propName might be different from real colum name. get info from property
    var hasAtLeastOne = false;
    for (let propName in entity.properties) {
      if (result[colPrefix + propName] !== undefined) {
        hasAtLeastOne = true;
        break;
      }
    }
    if (!hasAtLeastOne) {
      return null;
    }

    var item = {};

    for (let propName in entity.properties) {
      const prop = entity.properties[propName];
      if (prop.fk != null) {
        //todo: check if type is string
        item[propName] = result[colPrefix + prop.fk] == null
          ? null
          : BLOrm.fixResult(prop.type, result, propName + "_");
      } else {
        item[propName] = result[colPrefix + propName];
      }
    }

    return item;
  }

  static getInsertStatement(entity, data) {
    //INSERT INTO `perfumes`.`perfumes` (`name`, `factory_code`, `quantiy`, `supplier_id`, `supplier_code`) VALUES ('dokimi', '4584', '5456', '1', '888');
    var columns = "";
    var values = "";

    for (let p in entity.properties) {
      const propDef = entity.properties[p];
      if (data[p] == null || propDef.isKey) continue;
      let propName = BLOrm.getPropertyName(p, propDef);
      if (columns != "") {
        columns += ", ";
        values += ", ";
      }

      //todo: get type
      //todo: not karfoto id
      columns += "`" + propName + "`";
      values += propDef.fk != null
        ? data[p]["id"]
        : BLOrm._sqlizeValue(propDef, data[p]);
    }

    return `INSERT INTO ${entity.tableName} (${columns}) VALUES (${values});`
  }

  static getPropertyName(key, def) {
    return def.fk != null ? def.fk : def.columnName || key;
  }

  static query(entity) {
    return new Query(entity);
  }

  static startTransaction() {
    return new Transaction();
  }

}

module.exports = BLOrm;