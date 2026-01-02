/**
 * Database Manager Validation Utilities Tests
 */

import {
  validateDatabaseName,
  validateSchemaName,
  isValidDatabaseName,
  isValidSchemaName,
} from '../../db-manager/validation.js';

describe('db-manager validation', () => {
  describe('validateDatabaseName', () => {
    describe('valid names', () => {
      it('should accept lowercase letters', () => {
        expect(() => validateDatabaseName('mydb')).not.toThrow();
      });

      it('should accept uppercase letters', () => {
        expect(() => validateDatabaseName('MYDB')).not.toThrow();
      });

      it('should accept mixed case', () => {
        expect(() => validateDatabaseName('MyDatabase')).not.toThrow();
      });

      it('should accept underscores', () => {
        expect(() => validateDatabaseName('my_db')).not.toThrow();
      });

      it('should accept hyphens', () => {
        expect(() => validateDatabaseName('my-db')).not.toThrow();
      });

      it('should accept leading underscore', () => {
        expect(() => validateDatabaseName('_mydb')).not.toThrow();
      });

      it('should accept digits after first character', () => {
        expect(() => validateDatabaseName('db123')).not.toThrow();
      });

      it('should accept common database names', () => {
        expect(() => validateDatabaseName('postgres')).not.toThrow();
        expect(() => validateDatabaseName('production_db')).not.toThrow();
        expect(() => validateDatabaseName('dev-db')).not.toThrow();
      });
    });

    describe('invalid names', () => {
      it('should reject names starting with digit', () => {
        expect(() => validateDatabaseName('1db')).toThrow();
      });

      it('should reject names with semicolons (SQL injection)', () => {
        expect(() => validateDatabaseName('db;DROP TABLE')).toThrow();
      });

      it('should reject names with comment sequences (SQL injection)', () => {
        expect(() => validateDatabaseName('db--comment')).toThrow();
      });

      it('should reject names with single quotes', () => {
        expect(() => validateDatabaseName("db'name")).toThrow();
      });

      it('should reject names with double quotes', () => {
        expect(() => validateDatabaseName('db"name')).toThrow();
      });

      it('should reject names with backticks', () => {
        expect(() => validateDatabaseName('db`name')).toThrow();
      });

      it('should reject names with spaces', () => {
        expect(() => validateDatabaseName('my db')).toThrow();
      });

      it('should reject names with special characters', () => {
        expect(() => validateDatabaseName('db@name')).toThrow();
        expect(() => validateDatabaseName('db$name')).toThrow();
        expect(() => validateDatabaseName('db#name')).toThrow();
      });
    });
  });

  describe('validateSchemaName', () => {
    describe('valid names', () => {
      it('should accept lowercase letters', () => {
        expect(() => validateSchemaName('public')).not.toThrow();
      });

      it('should accept uppercase letters', () => {
        expect(() => validateSchemaName('PUBLIC')).not.toThrow();
      });

      it('should accept underscores', () => {
        expect(() => validateSchemaName('my_schema')).not.toThrow();
      });

      it('should accept leading underscore', () => {
        expect(() => validateSchemaName('_schema')).not.toThrow();
      });

      it('should accept digits after first character', () => {
        expect(() => validateSchemaName('schema123')).not.toThrow();
      });

      it('should accept common schema names', () => {
        expect(() => validateSchemaName('public')).not.toThrow();
        expect(() => validateSchemaName('information_schema')).not.toThrow();
        expect(() => validateSchemaName('pg_catalog')).not.toThrow();
      });
    });

    describe('invalid names', () => {
      it('should reject names starting with digit', () => {
        expect(() => validateSchemaName('1schema')).toThrow();
      });

      it('should reject names with hyphens', () => {
        expect(() => validateSchemaName('my-schema')).toThrow();
      });

      it('should reject names with spaces', () => {
        expect(() => validateSchemaName('my schema')).toThrow();
      });

      it('should reject names with special characters', () => {
        expect(() => validateSchemaName('schema@name')).toThrow();
        expect(() => validateSchemaName('schema$name')).toThrow();
      });

      it('should reject SQL injection attempts', () => {
        expect(() => validateSchemaName('public;DROP')).toThrow();
        expect(() => validateSchemaName("public'--")).toThrow();
      });
    });
  });

  describe('isValidDatabaseName', () => {
    it('should return true for valid names', () => {
      expect(isValidDatabaseName('mydb')).toBe(true);
      expect(isValidDatabaseName('my_db')).toBe(true);
      expect(isValidDatabaseName('my-db')).toBe(true);
    });

    it('should return false for invalid names', () => {
      expect(isValidDatabaseName('1db')).toBe(false);
      expect(isValidDatabaseName('db;DROP')).toBe(false);
      expect(isValidDatabaseName("db'name")).toBe(false);
    });
  });

  describe('isValidSchemaName', () => {
    it('should return true for valid names', () => {
      expect(isValidSchemaName('public')).toBe(true);
      expect(isValidSchemaName('my_schema')).toBe(true);
    });

    it('should return false for invalid names', () => {
      expect(isValidSchemaName('1schema')).toBe(false);
      expect(isValidSchemaName('my-schema')).toBe(false);
      expect(isValidSchemaName('schema;DROP')).toBe(false);
    });
  });
});
