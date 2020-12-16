package storage

import akka.actor.ActorSystem
import org.jooq.{DSLContext, SQLDialect}
import org.jooq.impl.DSL
import play.api.db.Database

import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}

class PostgresDatabase @Inject() (db: Database, system: ActorSystem) {
  val databaseContext: ExecutionContext = system.dispatchers.lookup("contexts.database")

  def query[A](block: DSLContext => A): Future[A] = Future {
    db.withConnection { connection =>
      block(DSL.using(connection, SQLDialect.POSTGRES))
    }
  }(databaseContext)

  def withTransation[A](block: DSLContext => A): Future[A] = Future {
    db.withTransaction { connection =>
      block(DSL.using(connection, SQLDialect.POSTGRES))
    }
  }(databaseContext)
}