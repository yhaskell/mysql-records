import * as mysqlconn from 'mysql';
import * as fs from 'fs';

type Constructor<T> = { new (...args: any[]): T } | ((...args: any[]) => T) | Function;

export class EmptyArrayError extends Error {
    constructor() { super("Cannot use $in with empty array"); }
}

export interface IConnection {
    query: (query: string, callback: (err?: mysqlconn.IError, ...values: any[]) => void) => void;
}

export class DB {
    
    private debugSQL: boolean;

    constructor(public connection: IConnection, config?: { sqlDebug?: boolean; }) {
        config = config || { sqlDebug: false };
        config.sqlDebug = config.sqlDebug || false;

        this.connection.query('set names utf8', ()=>{});
        
        this.debugSQL = config.sqlDebug;
    }

    private log: string[] = [];
    private errorLog: mysqlconn.IError[] = [];

    public getLog(error?: boolean) { return [].concat(error ? this.errorLog : this.log); }
    
    public get lastQuery() { return this.log[this.log.length - 1]; }
    public get lastError() { return this.errorLog[this.errorLog.length - 1]; }
    
    rotate(query: boolean, error: boolean) {
        if (query && this.log.length > 1000) {
            fs.appendFileSync('sql-query.log', '\n' + this.log.join('\n'));
            this.log = [];
        }
        if (error && this.errorLog.length > 1000) {
            var message = this.errorLog.map(x=>`[${x.errno}.${x.code}] ${x.name} -- ${x.message}`).join('\n');
            fs.appendFileSync('sql-error.log', '\n' + message);
            this.errorLog = []; 
        }
    }

    public query<T>(query: string, mapper?: (el: any) => T): Promise<T> {
        return new Promise((resolve, reject) => {
            
            this.log.push(query);
            this.rotate(true, false);
            if (this.debugSQL) console.log("[DEBUG] ", query);
            this.connection.query(query, (err, result) => {
                if (err) {
                    this.errorLog.push(err);
                    this.rotate(false, true);
                    reject(Object.assign(err, {query: query }));
                }
                else resolve(mapper? mapper(result): result);
        })});
    }

    public insert(table: string, obj) {
        var key2null = obj.key2null;
        delete obj.key2null;

        var keys = Object.keys(obj).filter(key => obj[key] !== undefined),
            values = keys.map(key => {
                var res = DB.convert(obj[key]);
                if (typeof res == "string") return res.replace(/"/g, '""');
                return res;
            }),
            qkeys = keys.map(x => "`" + x + "`");
        
        var query = `INSERT IGNORE INTO \`${table}\` (${qkeys.join(', ')}) values ("${values.join('", "')}")`;
        obj.key2null = key2null;
        return this.query<number>(query, res => res.affectedRows ? res.insertId: null);
    }
    
    public update(table: string, obj, nullKeys: string[], selector: string, ignore?: string[]) {
        var key2null = obj.key2null;
        delete obj.key2null;
        ignore = ignore || [];
        var keys = Object.keys(obj).filter(not => ignore.indexOf(not) == -1);

        var updates = keys.map(key => {
            var converted = DB.convert(obj[key]);
            if (typeof converted == "string") converted = converted.replace(/"/g, '""');
            return `\`${key}\` = "${converted}"`;
        });
        updates = updates.concat(nullKeys.map(key => `\`${key}\` = NULL`));
        if (updates.length == 0) return null;
        
        var query = `UPDATE \`${table}\` set ${updates.join(', ')} where ${selector}`;
        obj.key2null = key2null;
        return this.query<number>(query, res => res.affectedRows ? res.insertId: null);
    }
    
    public select<T>(table: string, clause?: string, mapper?: (el: any) => T): Promise<T[]> {
        var query = clause ? 
                    `SELECT * FROM \`${table}\` ${clause}` : 
                    `SELECT * FROM \`${table}\``;
        
        return this.query<T[]>(query, mapper ? res => res.map(mapper) : undefined);
    }
    
    public async selectOne<T>(table: string, clause?: string, mapper?: (el: any) => T): Promise<T> {
        var result = await this.select<T>(table, clause, mapper);
        return result[0];
    }
    
    public delete(table: string, clause?: string): Promise<number> {
        var query = `DELETE FROM \`${table}\` WHERE ${clause}`;
        return this.query<number>(query, res => res.affectedRows);
    }
    
    public async columns(table: string) : Promise<string[]> {
        return await this.query<string[]>(`show columns from ${table}`, columns => columns.map(c => c.Field));
    }
    
    public static convert (value) {
        if (typeof(value) == "string") return value;
        else if (typeof(value) == "boolean") return value ? 1 : 0;
        else if (value instanceof Date) {
            var date = <Date> value;
            var y = date.getFullYear();
            var m = date.getMonth() + 1;
            var d = date.getDate();
            var h = date.getHours();
            var mm = date.getMinutes();
            var s = date.getSeconds();
            var res = y + '-';
            if (m < 10) res += '0';
            res += m + '-';
            if (d < 10) res += '0';
            res += d + " ";
            if (h < 10) res += '0';
            res += h + ':';
            if (mm < 10) res += '0';
            res += mm + ':'
            if (s < 10) res += '0';
            return res + s;
        }
        else if (value instanceof Buffer) return value.toString();
        else return value;
    }

}

export function connect(host: string, user: string, password: string, db: string) {
    var pool = mysqlconn.createPool({
        host     : host,
        user     : user,
        password : password,
        database : db
    });
    return new DB(pool);
}

class ConnectionInfo {
    public connection: DB;
    public constructor(connection: DB | Promise<DB>, public tableName: string, public primaryKey?: string) {
        if (connection instanceof DB) this.connection = connection;
        else (<Promise<DB>>connection).then(value => this.connection = value);
    }
    public links: ConnectionLink[];
}

export type Filter<T> = { [K in keyof T]?: FilterSelector<T[K], T> };
export type FilterSelector<K, T> = K | { $gt: K; $lt: K; $gte: K; $lte: K; $ne: K; $in: K[]; }

class ConnectionLink {
    constructor(public foreignKey: string, public destination: Function, public source: any, public property: string) {}
    
    public test() {
        var dd: ConnectionInfo = this.destination.prototype.__db__;
        var dt = dd.tableName, dk = dd.primaryKey;
        var sd: ConnectionInfo = this.source.__db__;
        var st = sd.tableName, sk = this.foreignKey;
        
        if (!dk) throw new Error("Cannot link to tables without primary key");
    }
}

export class Model {
    
    private __db__: ConnectionInfo;
    
    
    public constructor(obj?: any) {
        if (!this.__db__) throw new Error('Use @model decorator to specify DB-specific information such as DB connection and table name');
        if (this.__db__.links) this.__db__.links.forEach(link => link.test());
        
        this.propagate(obj);
    }
    
    private async processLinks() {
        for (var j = 0; j < this.__db__.links.length; j++) {
            var link = this.__db__.links[j];
            if (this[link.foreignKey]) this[link.property] = await link.destination['get'](this[link.foreignKey]); 
        }
    }
    
    
    static all<T extends Model>(): Promise<T[]>  {
        return this.prototype.__db__.connection.select(this.prototype.__db__.tableName, '', x => new this().propagate(x));
    }
    
    private static selector(filter: any) {
        var terms = [];
        
        var operations = { 
            $gt: '>',
            $lt: '<',
            $gte: '>=',
            $lte: '<=',
            $ne: '!='
        }
        Object.keys(filter).forEach(key => {
            var fl = filter[key];
            if (fl && typeof(fl) == "object") 
                Object.keys(fl).forEach(fk => {
                    switch (fk) {
                        case "$in":
                            fl[fk] = fl[fk].filter(x => !!x);
                            if (fl[fk].length == 0) throw new EmptyArrayError();
                            terms.push(`(\`${key}\` in (${fl[fk].map(q=>DB.convert(q)).join(', ')}))`); 
                            break;
                        default:
                            terms.push(`(\`${key}\` ${operations[fk]} "${DB.convert(fl[fk])}")`)
                    }
                });
            else terms.push(`(\`${key}\` = "${DB.convert(fl)}")`);
        });
        
        return terms.join(" and ");
    }
    
    static find = async function <T extends Model>(this: Constructor<T>, filter: any): Promise<T[]> {
        var selector;
        var self: any = <any> this;
        try {
            selector = self.selector(filter);
        } catch (err) {
            if (err instanceof EmptyArrayError) return [];
            else throw err;
        }
        var found: T[] = await self.prototype.__db__.connection.select(this.prototype.__db__.tableName, `where ${selector}`, x => (new self()).propagate(x));
        if (!found) return [];
        for (var i = 0; i < found.length; i++)
            await found[i].processLinks(); 
        
        return found;
    }   
    
    static findOne = async function<T extends Model>(this: Constructor<T>, filter: any) : Promise<T> {
        var self = <any>this;
        var selector = Object.keys(filter).map(key => (`${key} = ${DB.convert(filter[key])}`)).join(" and ");
        var found = await self.prototype.__db__.connection.selectOne(this.prototype.__db__.tableName, `where ${self.selector(filter)}`, x=>new self().propagate(x));
        if (found)
            await found.processLinks();
        return found;
    }
    
    private key2null: string[] = [];

    setNull(key: string) { this.key2null.push(key); }

    public async save() {
        var db = this.__db__, pk = db.primaryKey, conn = db.connection, tbl = db.tableName, key2null = this.key2null;
        delete this.key2null;
        if (pk && this[pk]) 
            await conn.update(tbl, this, key2null, `${pk} = "${this[pk]}"`, [pk].concat(this.__db__.links.map(link=>link.property)));
        else {
            for (var key of key2null) delete this[key];
            this[pk] = await conn.insert(tbl, this);
        }
        this.key2null = [];
        await this.processLinks();
    }
    
    static get = function<T extends Model>(this: Constructor<T>, id: number): Promise<T> {
        var db = this.prototype.__db__;
        var pk = db.primaryKey; 
        var self: any = this; 
        if (pk == null) return new Promise((t, f) => { f(new Error("This operation is not available for models without primary key")) });
        var filter = {}; filter[pk] = id;
        return self.findOne(filter);
    }
    
    propagate(obj?: any) {
        if (!obj) return;
        
        var keys = Object.keys(obj);
        for (var i = 0; i < keys.length; i++)
            this[keys[i]] = DB.convert(obj[keys[i]]);
        return this;
    }
    
    delete() {
        var db = this.__db__, pk = db.primaryKey, conn = db.connection, tbl = db.tableName;
        
        if (!pk) return null;
        
        return conn.delete(tbl, `\`${pk}\` = ${this[pk]}`);
    }
    
    static remove(filter: any) {
        var db = this.prototype.__db__;
        
        var selector = this.selector(filter);

        return db.connection.delete(db.tableName, selector);
    }
}

export function model(db: DB | Promise<DB>, table: string) {
    return function(constructor: Function) {
        constructor.prototype.__db__ = new ConnectionInfo(db, table, constructor.prototype["__dbpk__"]);
        constructor.prototype.__db__.links = constructor.prototype["__dblinks__"] || [];
        
        delete constructor.prototype["__dbpk__"];
        delete constructor.prototype["__dblinks__"];
    }
}

export function primary(target: any, propertyKey: string) {
    target["__dbpk__"] = propertyKey;
}



export function link(destination: Function, key: string) {    
    return function (target: any, propertyKey: string) {
        var sd: ConnectionLink[] = target["__dblinks__"] || (target["__dblinks__"] = []);
        
        sd.push(new ConnectionLink(key, destination, target, propertyKey));
        
    };
}