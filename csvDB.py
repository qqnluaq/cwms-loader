import os
import sys
import tool

from csvkit import sql
from csvkit import table
from csvkit import CSVKitWriter

class DB( object ):
    def __init__( self ):
        self._connection = None

    @property
    def connection( self ):
        if self._connection:
            return self._connection

        self._connection = sql.get_connection( "sqlite:///:memory:" )
        return self._connection

    @property
    def engine( self ):
        return self.connection[ 0 ]

    @property
    def metadata( self ):
        return self.connection[ 1 ]

    def makeCsv( self, **arg ):
        return Csv( db=self, **arg )

    def makeQuery( self, **arg ):
        return Query( db=self, **arg )


class TableBase( object ):
    def __init__( self, db=None, name=None, inputs=None ):
        self._db = db

        if isinstance( inputs, list ):
            self._inputs = inputs
        else:
            self._inputs = [ inputs ]

        if name:
            self._name = name
        else:
            self._name = os.path.splitext( os.path.split( self.inputs[0] )[1] )[0]

        self._loaded = False
        self._count = 0

    @property
    def db( self ):
        return self._db

    @property
    def name( self ):
        return self._name

    @property
    def inputs( self ):
        return self._inputs

    @property
    def isLoaded( self ):
        return self._loaded

    @property
    def count( self ):
        return self._count

    def getRows( self ):
        if not self.isLoaded:
            if not self.fetch():
                raise 'failed to fetch ' + self.name

        connection = self.db.engine.connect()
        rows = connection.execute( 'select * from {}'.format( self.name ) )
        connection.close()

        tool.VERBOSE( 'read {} row(s) from {}', self.count, self.name )

        return rows

    def writeCsv( self, filename=None ):
        rows = self.getRows()

        if filename == None:
            filename = self.name + '.csv'

        # Output result of last query as CSV
        row_count = 0;
        with open( filename, 'wb' ) as out:
            output = CSVKitWriter( out )
            output.writerow( rows._metadata.keys )
            for row in rows:
                output.writerow(row)
                row_count += 1;

        tool.VERBOSE( 'wrote {} row(s) to csv {}', row_count, filename )


class Csv( TableBase ):
    def __init__( self, db=None, name=None, inputs=None ):
        super( Csv, self ).__init__( db = db, name = name, inputs = inputs )

        if len( self.inputs ) != 1:
            raise "exactly 1 input required"

    def fetch( self ):
        if self.isLoaded:
            tool.VERBOSE( 'use csv {}', self.name )
            return True

        tool.VERBOSE( 'fetching csv {}', self.name )

        with tool.INDENT():
            csv_table = table.Table.from_csv(
                open( self.inputs[ 0 ], 'rb' ),
                name            = self.name,
                # sniff_limit     = 0,
                blanks_as_nulls = True,
                infer_types     = True,
                no_header_row   = False
            )

            tool.VERBOSE( 'read {} row(s) from {}', csv_table.count_rows(), self.inputs[ 0 ] )

            connection = self.db.engine.connect()
            transaction = connection.begin()

            sql_table = sql.make_table(
                csv_table,
                self.name,
                False,
                None,
                self.db.metadata
            )

            sql_table.create()

            if csv_table.count_rows() > 0:
                insert = sql_table.insert()
                headers = csv_table.headers()
                connection.execute( insert, [dict(zip(headers, row)) for row in csv_table.to_rows()] )        

                self._count = csv_table.count_rows();
                tool.VERBOSE( 'wrote {} row(s) to table {}', self.count, self.name )

                self._loaded = True

            transaction.commit()
            connection.close()

        return self._loaded


class Query( TableBase ):
    def __init__( self, db=None, name=None, inputs=None, sql=None ):
        super( Query, self ).__init__( db = db, name = name, inputs = inputs )
        self._sql = sql

    @property
    def sql( self ):
        return self._sql

    def fetch( self ):
        if self.isLoaded:
            tool.VERBOSE( 'use query {}', self.name )
            return True

        tool.VERBOSE( 'fetching query {}', self.name )

        with tool.INDENT():
            for i in self.inputs:
                if isinstance( i, TableBase ):
                    if not i.fetch():
                        raise 'failed to fetch '+i.name

            connection = self.db.engine.connect()

            connection.execute( 'create table {} as {}'.format( self.name, self.sql ) )

            self._count = connection.execute( 'select count(*) as count from {}'.format( self.name ) ).scalar()

            connection.close()

            tool.VERBOSE( 'wrote {} row(s) to table {}', self.count, self.name )

        self._loaded = True 
        return self._loaded


