package storage

import akka.NotUsed
import akka.http.scaladsl.util.FastFuture
import akka.stream.Materializer
import akka.stream.scaladsl.{Framing, Keep, Sink, Source}
import akka.util.ByteString
import domain.record
import fr.maif.otoroshi.daikoku.domain._
import fr.maif.otoroshi.daikoku.domain.json._
import fr.maif.otoroshi.daikoku.env.Env
import org.jooq.{JSONB, Record}
import org.jooq.impl.DSL
import org.jooq.impl.DSL.{count, field}
import play.api.Logger
import play.api.libs.json._
import play.twirl.api.TwirlHelperImports.twirlJavaCollectionToScala

import scala.concurrent.{ExecutionContext, Future}

trait PostgresTenantCapableRepo[A, Id <: ValueType]
  extends TenantCapableRepo[A, Id] {

  def repo(): PostgresRepo[A, Id]

  def tenantRepo(tenant: TenantId): PostgresTenantAwareRepo[A, Id]

  override def forTenant(tenant: TenantId): Repo[A, Id] = tenantRepo(tenant)

  override def forTenantF(tenant: TenantId): Future[Repo[A, Id]] =
    Future.successful(tenantRepo(tenant))

  override def forAllTenant(): Repo[A, Id] = repo()

  override def forAllTenantF(): Future[Repo[A, Id]] = Future.successful(repo())
}

case class PostgresTenantCapableTeamRepo(
                                       _repo: () => PostgresRepo[Team, TeamId],
                                       _tenantRepo: TenantId => PostgresTenantAwareRepo[Team, TeamId])
  extends PostgresTenantCapableRepo[Team, TeamId]
    with TeamRepo {
  override def tenantRepo(
                           tenant: TenantId): PostgresTenantAwareRepo[Team, TeamId] =
    _tenantRepo(tenant)

  override def repo(): PostgresRepo[Team, TeamId] = _repo()
}

case class PostgresTenantCapableApiRepo(
                                      _repo: () => PostgresRepo[Api, ApiId],
                                      _tenantRepo: TenantId => PostgresTenantAwareRepo[Api, ApiId])
  extends PostgresTenantCapableRepo[Api, ApiId]
    with ApiRepo {
  override def tenantRepo(tenant: TenantId): PostgresTenantAwareRepo[Api, ApiId] =
    _tenantRepo(tenant)

  override def repo(): PostgresRepo[Api, ApiId] = _repo()
}

case class PostgresTenantCapableApiSubscriptionRepo(
                                                  _repo: () => PostgresRepo[ApiSubscription, ApiSubscriptionId],
                                                  _tenantRepo: TenantId => PostgresTenantAwareRepo[ApiSubscription,
                                                    ApiSubscriptionId]
                                                ) extends PostgresTenantCapableRepo[ApiSubscription, ApiSubscriptionId]
  with ApiSubscriptionRepo {
  override def tenantRepo(tenant: TenantId)
  : PostgresTenantAwareRepo[ApiSubscription, ApiSubscriptionId] =
    _tenantRepo(tenant)

  override def repo(): PostgresRepo[ApiSubscription, ApiSubscriptionId] = _repo()
}

case class PostgresTenantCapableApiDocumentationPageRepo(
                                                       _repo: () => PostgresRepo[ApiDocumentationPage, ApiDocumentationPageId],
                                                       _tenantRepo: TenantId => PostgresTenantAwareRepo[ApiDocumentationPage,
                                                         ApiDocumentationPageId]
                                                     ) extends PostgresTenantCapableRepo[ApiDocumentationPage, ApiDocumentationPageId]
  with ApiDocumentationPageRepo {
  override def tenantRepo(tenant: TenantId)
  : PostgresTenantAwareRepo[ApiDocumentationPage, ApiDocumentationPageId] =
    _tenantRepo(tenant)

  override def repo(): PostgresRepo[ApiDocumentationPage, ApiDocumentationPageId] =
    _repo()
}

case class PostgresTenantCapableNotificationRepo(
                                               _repo: () => PostgresRepo[Notification, NotificationId],
                                               _tenantRepo: TenantId => PostgresTenantAwareRepo[Notification, NotificationId]
                                             ) extends PostgresTenantCapableRepo[Notification, NotificationId]
  with NotificationRepo {
  override def tenantRepo(
                           tenant: TenantId): PostgresTenantAwareRepo[Notification, NotificationId] =
    _tenantRepo(tenant)

  override def repo(): PostgresRepo[Notification, NotificationId] = _repo()
}

case class PostgresTenantCapableAuditTrailRepo(
                                             _repo: () => PostgresRepo[JsObject, Int],
                                             _tenantRepo: TenantId => PostgresTenantAwareRepo[JsObject, Int])
  extends PostgresTenantCapableRepo[JsObject, Int]
    with AuditTrailRepo {
  override def tenantRepo(
                           tenant: TenantId): PostgresTenantAwareRepo[JsObject, Int] =
    _tenantRepo(tenant)

  override def repo(): PostgresRepo[JsObject, Int] = _repo()
}

case class PostgresTenantCapableTranslationRepo(
                                              _repo: () => PostgresRepo[Translation, Int],
                                              _tenantRepo: TenantId => PostgresTenantAwareRepo[Translation, Int]
                                            ) extends PostgresTenantCapableRepo[Translation, Int]
  with TranslationRepo {
  override def tenantRepo(
                           tenant: TenantId): PostgresTenantAwareRepo[Translation, Int] =
    _tenantRepo(tenant)

  override def repo(): PostgresRepo[Translation, Int] = _repo()
}

case class PostgresTenantCapableMessageRepo(
                                          _repo: () => PostgresRepo[Message, Int],
                                          _tenantRepo: TenantId => PostgresTenantAwareRepo[Message, Int]
                                        ) extends PostgresTenantCapableRepo[Message, Int]
  with MessageRepo {
  override def tenantRepo(
                           tenant: TenantId): PostgresTenantAwareRepo[Message, Int] =
    _tenantRepo(tenant)

  override def repo(): PostgresRepo[Message, Int] = _repo()
}

case class PostgresTenantCapableConsumptionRepo(
                                              _repo: () => PostgresRepo[ApiKeyConsumption, Int],
                                              _tenantRepo: TenantId => PostgresTenantAwareRepo[ApiKeyConsumption, Int]
                                            ) extends PostgresTenantCapableRepo[ApiKeyConsumption, Int]
  with ConsumptionRepo {

  implicit val jsObjectFormat: OFormat[JsObject] = new OFormat[JsObject] {
    override def reads(json: JsValue): JsResult[JsObject] =
      json.validate[JsObject](Reads.JsObjectReads)

    override def writes(o: JsObject): JsObject = o
  }

  val jsObjectWrites: OWrites[JsObject] = (o: JsObject) => o

  override def tenantRepo(
                           tenant: TenantId): PostgresTenantAwareRepo[ApiKeyConsumption, Int] =
    _tenantRepo(tenant)

  override def repo(): PostgresRepo[ApiKeyConsumption, Int] = _repo()

  private def lastConsumptions(tenantId: Option[TenantId], filter: JsObject)(
    implicit ec: ExecutionContext): Future[Seq[ApiKeyConsumption]] = ???
//  {
//    val rep = tenantId match {
//      case Some(t) =>
//        forTenant(t)
//          .asInstanceOf[PostgresTenantAwareRepo[ApiKeyConsumption, Int]]
//      case None =>
//        forAllTenant().asInstanceOf[PostgresRepo[ApiKeyConsumption, Int]]
//    }
//
//    rep.collection.flatMap { col =>
//      import col.BatchCommands.AggregationFramework
//      import AggregationFramework.{Group, Match, MaxField}
//
//      col
//        .aggregatorContext[JsObject](
//          firstOperator = Match(filter),
//          otherOperators =
//            List(Group(JsString("$clientId"))("maxFrom" -> MaxField("from"))),
//          explain = false,
//          allowDiskUse = false,
//          bypassDocumentValidation = false,
//          readConcern = ReadConcern.Majority,
//          readPreference = ReadPreference.primaryPreferred,
//          writeConcern = WriteConcern.Default,
//          batchSize = None,
//          cursorOptions = CursorOptions.empty,
//          maxTime = None,
//          hint = None,
//          comment = None,
//          collation = None
//        )
//        .prepared
//        .cursor
//        .collect[List](-1, Cursor.FailOnError[List[JsObject]]())
//        .flatMap { agg =>
//          val futures: List[Future[Option[ApiKeyConsumption]]] = agg.map(
//            json =>
//              col
//                .find(Json.obj("clientId" -> (json \ "_id").as[String],
//                  "from" -> (json \ "maxFrom" \ "$long").as[Long]),
//                  None)
//                .one[JsObject](ReadPreference.primaryPreferred)
//                .map { results =>
//                  results.map(rep.format.reads).collect {
//                    case JsSuccess(e, _) => e
//                  }
//                }
//          )
//          Future.sequence(futures).map(_.flatten)
//        }
//    }
//  }

  override def getLastConsumptionsforAllTenant(filter: JsObject)(
    implicit ec: ExecutionContext
  ): Future[Seq[ApiKeyConsumption]] = lastConsumptions(None, filter)

  override def getLastConsumptionsForTenant(tenantId: TenantId,
                                            filter: JsObject)(
                                             implicit ec: ExecutionContext
                                           ): Future[Seq[ApiKeyConsumption]] = lastConsumptions(Some(tenantId), filter)
}

class PostgresDataStore(env: Env, db: PostgresDatabase)
  extends DataStore {

  val logger: Logger = Logger("PostgresDataStore")
  implicit val ec: ExecutionContext = env.defaultExecutionContext

  private val _tenantRepo: TenantRepo =
    new PostgresTenantRepo(db)
  private val _userRepo: UserRepo = new PostgresUserRepo(db)
  private val _teamRepo: TeamRepo = PostgresTenantCapableTeamRepo(
    () => new PostgresTeamRepo(db),
    t => new PostgresTenantTeamRepo(db, t))
  private val _apiRepo: ApiRepo = PostgresTenantCapableApiRepo(
    () => new PostgresApiRepo(db),
    t => new PostgresTenantApiRepo(db, t))
  private val _apiSubscriptionRepo: ApiSubscriptionRepo =
    PostgresTenantCapableApiSubscriptionRepo(
      () => new PostgresApiSubscriptionRepo(db),
      t => new PostgresTenantApiSubscriptionRepo(db, t)
    )
  private val _apiDocumentationPageRepo: ApiDocumentationPageRepo =
    PostgresTenantCapableApiDocumentationPageRepo(
      () => new PostgresApiDocumentationPageRepo(db),
      t => new PostgresTenantApiDocumentationPageRepo(db, t)
    )
  private val _notificationRepo: NotificationRepo =
    PostgresTenantCapableNotificationRepo(
      () => new PostgresNotificationRepo(db),
      t => new PostgresTenantNotificationRepo(db, t)
    )
  private val _userSessionRepo: UserSessionRepo =
    new PostgresUserSessionRepo(db)
  private val _auditTrailRepo: AuditTrailRepo =
    PostgresTenantCapableAuditTrailRepo(
      () => new PostgresAuditTrailRepo(db),
      t => new PostgresTenantAuditTrailRepo(db, t)
    )
  private val _consumptionRepo: ConsumptionRepo =
    PostgresTenantCapableConsumptionRepo(
      () => new PostgresConsumptionRepo(db),
      t => new PostgresTenantConsumptionRepo(db, t)
    )
  private val _passwordResetRepo: PasswordResetRepo =
    new PostgresPasswordResetRepo(db)
  private val _accountCreationRepo: AccountCreationRepo =
    new PostgresAccountCreationRepo(db)
  private val _translationRepo: TranslationRepo =
    PostgresTenantCapableTranslationRepo(
      () => new PostgresTranslationRepo(db),
      t => new PostgresTenantTranslationRepo(db, t))
  private val _messageRepo: MessageRepo =
    PostgresTenantCapableMessageRepo(
      () => new PostgresMessageRepo(db),
      t => new PostgresTenantMessageRepo(db, t)
    )

  override def tenantRepo: TenantRepo = _tenantRepo

  override def userRepo: UserRepo = _userRepo

  override def teamRepo: TeamRepo = _teamRepo

  override def apiRepo: ApiRepo = _apiRepo

  override def apiSubscriptionRepo: ApiSubscriptionRepo = _apiSubscriptionRepo

  override def apiDocumentationPageRepo: ApiDocumentationPageRepo =
    _apiDocumentationPageRepo

  override def notificationRepo: NotificationRepo = _notificationRepo

  override def userSessionRepo: UserSessionRepo = _userSessionRepo

  override def auditTrailRepo: AuditTrailRepo = _auditTrailRepo

  override def consumptionRepo: ConsumptionRepo = _consumptionRepo

  override def passwordResetRepo: PasswordResetRepo = _passwordResetRepo

  override def accountCreationRepo: AccountCreationRepo = _accountCreationRepo

  override def translationRepo: TranslationRepo = _translationRepo

  override def messageRepo: MessageRepo = _messageRepo

  override def start(): Future[Unit] = Future.successful(())

  override def stop(): Future[Unit] = Future.successful(())

  override def isEmpty(): Future[Boolean] = {

    for {
      tenants <- tenantRepo.count()
    } yield {
      tenants == 0
    }
  }

  override def exportAsStream(pretty: Boolean)(
    implicit ec: ExecutionContext,
    mat: Materializer,
    env: Env): Source[ByteString, _] = {
    val collections: List[Repo[_, _]] = List(
      tenantRepo,
      userRepo,
      passwordResetRepo,
      accountCreationRepo,
      userSessionRepo
    ) ++ List(
      teamRepo.forAllTenant(),
      apiRepo.forAllTenant(),
      apiSubscriptionRepo.forAllTenant(),
      apiDocumentationPageRepo.forAllTenant(),
      notificationRepo.forAllTenant(),
      auditTrailRepo.forAllTenant(),
      consumptionRepo.forAllTenant(),
      translationRepo.forAllTenant()
    )
    Source(collections).flatMapConcat { collection =>
      collection.streamAllRaw().map { doc =>
        if (pretty) {
          ByteString(
            Json.prettyPrint(Json.obj("type" -> collection.tableName,
              "payload" -> doc)) + "\n")
        } else {
          ByteString(
            Json.stringify(Json.obj("type" -> collection.tableName,
              "payload" -> doc)) + "\n")
        }
      }
    }
  }

  override def importFromStream(source: Source[ByteString, _])(
    implicit ec: ExecutionContext,
    mat: Materializer,
    env: Env): Future[Unit] = {
    for {
      _ <- env.dataStore.tenantRepo.deleteAll()
      _ <- env.dataStore.passwordResetRepo.deleteAll()
      _ <- env.dataStore.accountCreationRepo.deleteAll()
      _ <- env.dataStore.userRepo.deleteAll()
      _ <- env.dataStore.teamRepo.forAllTenant().deleteAll()
      _ <- env.dataStore.apiRepo.forAllTenant().deleteAll()
      _ <- env.dataStore.apiSubscriptionRepo.forAllTenant().deleteAll()
      _ <- env.dataStore.apiDocumentationPageRepo.forAllTenant().deleteAll()
      _ <- env.dataStore.notificationRepo.forAllTenant().deleteAll()
      _ <- env.dataStore.consumptionRepo.forAllTenant().deleteAll()
      _ <- env.dataStore.auditTrailRepo.forAllTenant().deleteAll()
      _ <- env.dataStore.userSessionRepo.deleteAll()
      _ <- env.dataStore.translationRepo.forAllTenant().deleteAll()
      _ <- source
        .via(Framing.delimiter(ByteString("\n"), 1000000000, true))
        .map(_.utf8String)
        .map(Json.parse)
        .map(json => json.as[JsObject])
        .map(json =>
          ((json \ "type").as[String], (json \ "payload").as[JsValue]))
        .mapAsync(1) {
          case ("Tenants", payload) =>
            env.dataStore.tenantRepo.save(TenantFormat.reads(payload).get)
          case ("PasswordReset", payload) =>
            env.dataStore.passwordResetRepo.save(
              PasswordResetFormat.reads(payload).get)
          case ("AccountCreation", payload) =>
            env.dataStore.accountCreationRepo.save(
              AccountCreationFormat.reads(payload).get)
          case ("Users", payload) =>
            env.dataStore.userRepo.save(UserFormat.reads(payload).get)
          case ("Teams", payload) =>
            env.dataStore.teamRepo
              .forAllTenant()
              .save(TeamFormat.reads(payload).get)
          case ("Apis", payload) =>
            env.dataStore.apiRepo
              .forAllTenant()
              .save(ApiFormat.reads(payload).get)
          case ("ApiSubscriptions", payload) =>
            env.dataStore.apiSubscriptionRepo
              .forAllTenant()
              .save(ApiSubscriptionFormat.reads(payload).get)
          case ("ApiDocumentationPages", payload) =>
            env.dataStore.apiDocumentationPageRepo
              .forAllTenant()
              .save(ApiDocumentationPageFormat.reads(payload).get)
          case ("Notifications", payload) =>
            env.dataStore.notificationRepo
              .forAllTenant()
              .save(NotificationFormat.reads(payload).get)
          case ("Consumptions", payload) =>
            env.dataStore.consumptionRepo
              .forAllTenant()
              .save(ConsumptionFormat.reads(payload).get)
          case ("Translations", payload) =>
            env.dataStore.translationRepo
              .forAllTenant()
              .save(TranslationFormat.reads(payload).get)
          case ("AuditEvents", payload) =>
            env.dataStore.auditTrailRepo
              .forAllTenant()
              .save(payload.as[JsObject])
          case ("UserSessions", payload) =>
            env.dataStore.userSessionRepo.save(
              UserSessionFormat.reads(payload).get)
          case (typ, _) =>
            logger.info(s"Unknown type: $typ")
            FastFuture.successful(false)
        }
        .toMat(Sink.ignore)(Keep.right)
        .run()
    } yield ()

  }
}

class PostgresTenantRepo(db: PostgresDatabase)
  extends PostgresRepo[Tenant, TenantId](db)
    with TenantRepo {
  override def tableName: String = "Tenants"

  override def format: Format[Tenant] = json.TenantFormat

  override def formatRecord: Format[Tenant] = record.TenantFormat

  override def extractId(value: Tenant): String = value.id.value
}

class PostgresPasswordResetRepo(db: PostgresDatabase)
  extends PostgresRepo[PasswordReset, Int](db)
    with PasswordResetRepo {
  override def tableName: String = "PasswordReset"

  override def format: Format[PasswordReset] = json.PasswordResetFormat

  override def formatRecord: Format[PasswordReset] = record.PasswordResetFormat

  override def extractId(value: PasswordReset): String = value.id.value
}

class PostgresAccountCreationRepo(db: PostgresDatabase)
  extends PostgresRepo[AccountCreation, Int](db)
    with AccountCreationRepo {
  override def tableName: String = "AccountCreation"

  override def format: Format[AccountCreation] = json.AccountCreationFormat

  override def formatRecord: Format[AccountCreation] = record.AccountCreationFormat

  override def extractId(value: AccountCreation): String = value.id.value
}

class PostgresTenantTeamRepo(db: PostgresDatabase, tenant: TenantId)
  extends PostgresTenantAwareRepo[Team, TeamId](db, tenant) {
  override def tableName: String = "Teams"

  override def format: Format[Team] = json.TeamFormat

  override def formatRecord: Format[Team] = record.TeamFormat

  override def extractId(value: Team): String = value.id.value
}

class PostgresTenantApiRepo(db: PostgresDatabase, tenant: TenantId)
  extends PostgresTenantAwareRepo[Api, ApiId](db, tenant) {
  override def tableName: String = "Apis"

  override def format: Format[Api] = json.ApiFormat

  override def formatRecord: Format[Api] = record.ApiFormat

  override def extractId(value: Api): String = value.id.value
}

class PostgresTenantTranslationRepo(
                                 db: PostgresDatabase,
                                 tenant: TenantId)
  extends PostgresTenantAwareRepo[Translation, Int](db, tenant) {
  override def tableName: String = "Translations"

  override def format: Format[Translation] = json.TranslationFormat

  override def formatRecord: Format[Translation] = record.TranslationFormat

  override def extractId(value: Translation): String = value.id.value
}

class PostgresTenantMessageRepo(db: PostgresDatabase,  tenant: TenantId)
  extends PostgresTenantAwareRepo[Message, Int](db, tenant) {
  override def tableName: String = "Messages"

  override def format: Format[Message] = json.MessageFormat

  override def formatRecord: Format[Message] = record.MessageFormat

  override def extractId(value: Message): String = value.id.value
}

class PostgresTenantApiSubscriptionRepo(db: PostgresDatabase,
                                     tenant: TenantId)
  extends PostgresTenantAwareRepo[ApiSubscription, ApiSubscriptionId](db, tenant) {
  override def tableName: String = "ApiSubscriptions"

  override def format: Format[ApiSubscription] = json.ApiSubscriptionFormat

  override def formatRecord: Format[ApiSubscription] = record.ApiSubscriptionFormat

  override def extractId(value: ApiSubscription): String = value.id.value
}

class PostgresTenantApiDocumentationPageRepo(db: PostgresDatabase,
                                          tenant: TenantId)
  extends PostgresTenantAwareRepo[ApiDocumentationPage, ApiDocumentationPageId](
    db,
    tenant) {
  override def tableName: String = "ApiDocumentationPages"

  override def format: Format[ApiDocumentationPage] =
    json.ApiDocumentationPageFormat

  override def formatRecord: Format[ApiDocumentationPage] = record.ApiDocumentationPageFormat

  override def extractId(value: ApiDocumentationPage): String = value.id.value
}

class PostgresTenantNotificationRepo(db: PostgresDatabase,
                                  tenant: TenantId)
  extends PostgresTenantAwareRepo[Notification, NotificationId](db,
    tenant) {
  override def tableName: String = "Notifications"

  override def format: Format[Notification] =
    json.NotificationFormat

  override def formatRecord: Format[Notification] = record.NotificationFormat

  override def extractId(value: Notification): String = value.id.value
}

class PostgresTenantConsumptionRepo(db: PostgresDatabase,
                                 tenant: TenantId)
  extends PostgresTenantAwareRepo[ApiKeyConsumption, Int](db, tenant) {
  override def tableName: String = "Consumptions"

  override def format: Format[ApiKeyConsumption] =
    json.ConsumptionFormat

  override def formatRecord: Format[ApiKeyConsumption] = record.ConsumptionFormat

  override def extractId(value: ApiKeyConsumption): String = value.id.value
}

class PostgresTenantAuditTrailRepo(db: PostgresDatabase,
                                tenant: TenantId)
  extends PostgresTenantAwareRepo[JsObject, Int](db,
    tenant) {
  val _fmt = new Format[JsObject] {
    override def reads(json: JsValue): JsResult[JsObject] =
      JsSuccess(json.as[JsObject])

    override def writes(o: JsObject): JsValue = o
  }

  override def tableName: String = "AuditEvents"

  override def format: Format[JsObject] = _fmt

  override def formatRecord: Format[JsObject] = _fmt

  override def extractId(value: JsObject): String = (value \ "_id").as[String]
}

class PostgresUserRepo(db: PostgresDatabase)
  extends PostgresRepo[User, UserId](db)
    with UserRepo {
  override def tableName: String = "Users"

  override def format: Format[User] = json.UserFormat

  override def formatRecord: Format[User] = record.UserFormat

  override def extractId(value: User): String = value.id.value
}

class PostgresTeamRepo(db: PostgresDatabase)
  extends PostgresRepo[Team, TeamId](db) {
  override def tableName: String = "Teams"

  override def format: Format[Team] = json.TeamFormat

  override def formatRecord: Format[Team] = record.TeamFormat

  override def extractId(value: Team): String = value.id.value
}

class PostgresTranslationRepo(db: PostgresDatabase)
  extends PostgresRepo[Translation, Int](db) {
  override def tableName: String = "Translations"

  override def format: Format[Translation] = json.TranslationFormat

  override def formatRecord: Format[Translation] = record.TranslationFormat

  override def extractId(value: Translation): String = value.id.value
}

class PostgresMessageRepo(db: PostgresDatabase)
  extends PostgresRepo[Message, Int](db) {
  override def tableName: String = "Messages"

  override def format: Format[Message] = json.MessageFormat

  override def formatRecord: Format[Message] = record.MessageFormat

  override def extractId(value: Message): String = value.id.value
}

class PostgresApiRepo(db: PostgresDatabase)
  extends PostgresRepo[Api, ApiId](db) {
  override def tableName: String = "Apis"

  override def format: Format[Api] = json.ApiFormat

  override def formatRecord: Format[Api] = record.ApiFormat

  override def extractId(value: Api): String = value.id.value
}

class PostgresApiSubscriptionRepo(db: PostgresDatabase)
  extends PostgresRepo[ApiSubscription, ApiSubscriptionId](db) {
  override def tableName: String = "ApiSubscriptions"

  override def format: Format[ApiSubscription] = json.ApiSubscriptionFormat

  override def formatRecord: Format[ApiSubscription] = record.ApiSubscriptionFormat

  override def extractId(value: ApiSubscription): String = value.id.value
}

class PostgresApiDocumentationPageRepo(db: PostgresDatabase)
  extends PostgresRepo[ApiDocumentationPage, ApiDocumentationPageId](
    db) {
  override def tableName: String = "ApiDocumentationPages"

  override def format: Format[ApiDocumentationPage] =
    json.ApiDocumentationPageFormat

  override def formatRecord: Format[ApiDocumentationPage] = record.ApiDocumentationPageFormat

  override def extractId(value: ApiDocumentationPage): String = value.id.value
}

class PostgresNotificationRepo(db: PostgresDatabase)
  extends PostgresRepo[Notification, NotificationId](db) {
  override def tableName: String = "Notifications"

  override def format: Format[Notification] =
    json.NotificationFormat

  override def formatRecord: Format[Notification] = record.NotificationFormat

  override def extractId(value: Notification): String = value.id.value
}

class PostgresConsumptionRepo(db: PostgresDatabase)
  extends PostgresRepo[ApiKeyConsumption, Int](db) {
  override def tableName: String = "Consumptions"

  override def format: Format[ApiKeyConsumption] = json.ConsumptionFormat

  override def formatRecord: Format[ApiKeyConsumption] = record.ConsumptionFormat

  override def extractId(value: ApiKeyConsumption): String = value.id.value
}

class PostgresUserSessionRepo(db: PostgresDatabase)
  extends PostgresRepo[UserSession, Int](db)
    with UserSessionRepo {
  override def tableName: String = "UserSessions"

  override def format: Format[UserSession] =
    json.UserSessionFormat

  override def formatRecord: Format[UserSession] = record.UserSessionFormat

  override def extractId(value: UserSession): String = value.id.value
}

class PostgresAuditTrailRepo(db: PostgresDatabase)
  extends PostgresRepo[JsObject, Int](db) {
  val _fmt = new Format[JsObject] {
    override def reads(json: JsValue): JsResult[JsObject] =
      JsSuccess(json.as[JsObject])

    override def writes(o: JsObject): JsValue = o
  }

  override def tableName: String = "AuditEvents"

  override def format: Format[JsObject] = _fmt

  override def formatRecord: Format[JsObject] = _fmt

  override def extractId(value: JsObject): String = (value \ "_id").as[String]
}

abstract class PostgresRepo[Of, Id <: ValueType](db: PostgresDatabase)
  extends Repo[Of, Id] {

  val logger = Logger(s"PostgresRepo")

  implicit val jsObjectFormat = new OFormat[JsObject] {
    override def reads(json: JsValue): JsResult[JsObject] = json.validate[JsObject](Reads.JsObjectReads)
    override def writes(o: JsObject): JsObject = o
  }

  def queryToWhereCondition(query: JsObject) = {
    query.fields.map { field =>
      s"$field._1 = $field._2"
    }.reduce((x, y) => s"$x AND $y")
  }

  def tableName: String

  override def find(
                     query: JsObject,
                     sort: Option[JsObject] = None,
                     maxDocs: Int = -1)(implicit ec: ExecutionContext): Future[Seq[Of]] = ???
//    collection.flatMap { col =>
//      logger.debug(s"$tableName.find(${Json.prettyPrint(query)})")
//      sort match {
//        case None =>
//          col
//            .find(query, None)
//            .cursor[JsObject](ReadPreference.primaryPreferred)
//            .collect[Seq](maxDocs = maxDocs,
//              Cursor.FailOnError[Seq[JsObject]]())
//            .map(_.map(format.reads).collect {
//              case JsSuccess(e, _) => e
//            })
//        case Some(s) =>
//          col
//            .find(query, None)
//            .sort(s)
//            .cursor[JsObject](ReadPreference.primaryPreferred)
//            .collect[Seq](maxDocs = maxDocs,
//              Cursor.FailOnError[Seq[JsObject]]())
//            .map(_.map(format.reads).collect {
//              case JsSuccess(e, _) => e
//            })
//      }
//    }

  override def count(query: JsObject)(
    implicit ec: ExecutionContext): Future[Int] =
    db.query[Int] { sql =>
       sql
         .query(s"select count(*) from $tableName")
         .execute()
    }

  def findAllRaw()(implicit ec: ExecutionContext): Future[Seq[JsValue]] = ???
//  {
//      logger.debug(s"$tableName.findAllRaw({})")
//      db.query { sql =>
//        sql
//          .selectFrom(tableName)
//          .fetchInto(classOf[JsonRecord])
//          .toSeq
//          .map(Json.toJson(_))
//      }
//  }

//  col
//        .find(Json.obj(), None)
//        .cursor[JsObject](ReadPreference.primaryPreferred)
//        .collect[Seq](maxDocs = -1, Cursor.FailOnError[Seq[JsObject]]())

  def streamAll()(implicit ec: ExecutionContext): Source[Of, NotUsed] = ???
//    Source
//      .future(collection.flatMap { col =>
//        logger.debug(s"$tableName.streamAll({})")
//        col
//          .find(Json.obj(), None)
//          .cursor[JsObject](ReadPreference.primaryPreferred)
//          .collect[Seq](maxDocs = -1, Cursor.FailOnError[Seq[JsObject]]())
//          .map { docs =>
//            docs
//              .map(format.reads)
//              .map {
//                case j @ JsSuccess(_, _) => j
//                case j @ JsError(_) =>
//                  println(s"error: $j")
//                  j
//              }
//              .collect {
//                case JsSuccess(e, _) => e
//              }
//          }
//      })
//      .flatMapConcat(seq => Source(seq.toList))

  def streamAllRaw()(implicit ec: ExecutionContext): Source[JsValue, NotUsed] = ???
//    Source
//      .future(collection.flatMap { col =>
//        logger.debug(s"$tableName.streamAllRaw({})")
//        col
//          .find(Json.obj(), None)
//          .cursor[JsObject](ReadPreference.primaryPreferred)
//          .collect[Seq](maxDocs = -1, Cursor.FailOnError[Seq[JsObject]]())
//      })
//      .flatMapConcat(seq => Source(seq.toList))

  override def findOne(query: JsObject)(
    implicit ec: ExecutionContext): Future[Option[Of]] = {
    logger.debug(s"$tableName.findOne(${Json.prettyPrint(query)})")
    db.query { sql =>
      sql
        .selectFrom(tableName)
        .where(queryToWhereCondition(query))
        .fetchOne()
        .map(formatRecord.reads(_))
//        .collect {
//          case JsSuccess(e, _) => e
//        })
    }
  }

  override def delete(query: JsObject)(
    implicit ec: ExecutionContext): Future[Int] = collection.flatMap {
    col =>
      logger.debug(s"$tableName.delete(${Json.prettyPrint(query)})")
      col.delete(ordered = true).one(query)
  }

  override def save(value: Of)(
    implicit ec: ExecutionContext): Future[Boolean] = {
    val payload = format.writes(value).as[JsObject]
    save(Json.obj("_id" -> extractId(value)), payload)
  }

  override def save(query: JsObject, value: JsObject)(
    implicit ec: ExecutionContext): Future[Boolean] =
    collection.flatMap { col =>
      logger.debug(
        s"$tableName.upsert(${Json.prettyPrint(query)}, ${Json.prettyPrint(value)})"
      )
      col
        .findAndUpdate(
          selector = query,
          update = value,
          fetchNewObject = false,
          upsert = true,
          sort = None,
          fields = None,
          bypassDocumentValidation = false,
          writeConcern = WriteConcern.Default,
          maxTime = None,
          collation = None,
          arrayFilters = Seq.empty
        )
        .map(_.lastError.isDefined)
    }

  override def insertMany(values: Seq[Of])(
    implicit ec: ExecutionContext): Future[Long] =
    collection.flatMap { col =>
      val payloads = values.map(v => format.writes(v).as[JsObject])
      col
        .insert(true)
        .many(payloads)
        .map(_.n)
    }

  override def updateMany(query: JsObject, value: JsObject)(
    implicit ec: ExecutionContext): Future[Long] =
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update
        .element(q = query,
          u = Json.obj("$set" -> value),
          upsert = false,
          multi = true)
        .flatMap { element =>
          update.many(List(element)).map(_.nModified)
        }
    }

  override def updateManyByQuery(query: JsObject, queryUpdate: JsObject)(
    implicit ec: ExecutionContext): Future[Long] =
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update
        .element(q = query, u = queryUpdate, upsert = false, multi = true)
        .flatMap { element =>
          update.many(List(element)).map(_.nModified)
        }
    }

  override def deleteByIdLogically(id: String)(
    implicit ec: ExecutionContext): Future[Int] = {
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update.one(
        q = Json.obj("_deleted" -> false, "_id" -> id),
        u = Json.obj("$set" -> Json.obj("_deleted" -> true)),
        upsert = false,
        multi = false
      )
    }
  }

  override def deleteByIdLogically(id: Id)(
    implicit ec: ExecutionContext): Future[Int] = {
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update.one(
        q = Json.obj("_deleted" -> false, "_id" -> id.value),
        u = Json.obj("$set" -> Json.obj("_deleted" -> true)),
        upsert = false,
        multi = false
      )
    }
  }

  override def deleteLogically(query: JsObject)(
    implicit ec: ExecutionContext): Future[Boolean] = {
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update
        .element(q = query ++ Json.obj("_deleted" -> false),
          u = Json.obj("$set" -> Json.obj("_deleted" -> true)),
          upsert = false,
          multi = true)
        .flatMap { element =>
          update.many(List(element)).map(_.ok)
        }
    }
  }

  override def deleteAllLogically()(
    implicit ec: ExecutionContext): Future[Boolean] = {
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update
        .element(q = Json.obj("_deleted" -> false),
          u = Json.obj("$set" -> Json.obj("_deleted" -> true)),
          upsert = false,
          multi = true)
        .flatMap { element =>
          update.many(List(element)).map(_.ok)
        }
    }
  }

  override def exists(query: JsObject)(
    implicit ec: ExecutionContext): Future[Boolean] = collection.flatMap {
    col =>
      logger.debug(s"$tableName.exists(${Json.prettyPrint(query)})")
      col
        .find(query, None)
        .one[JsObject](ReadPreference.primaryPreferred)
        .map(_.isDefined)
  }

  override def count()(implicit ec: ExecutionContext): Future[Long] =
    collection.flatMap { col =>
      logger.debug(s"$tableName.count({})")
      col.count(None, None, 0, None, ReadConcern.Majority)
    }

  override def findWithProjection(query: JsObject, projection: JsObject)(
    implicit ec: ExecutionContext
  ): Future[Seq[JsObject]] = collection.flatMap { col =>
    logger.debug(
      s"$tableName.find(${Json.prettyPrint(query)}, ${Json.prettyPrint(projection)})")
    col
      .find(query, Some(projection))
      .cursor[JsObject](ReadPreference.primaryPreferred)
      .collect[Seq](maxDocs = -1, Cursor.FailOnError[Seq[JsObject]]())
  }

  override def findOneWithProjection(query: JsObject, projection: JsObject)(
    implicit ec: ExecutionContext
  ): Future[Option[JsObject]] = collection.flatMap { col =>
    logger.debug(
      s"$tableName.findOne(${Json.prettyPrint(query)}, ${Json.prettyPrint(projection)})")
    col
      .find(query, Some(projection))
      .one[JsObject](ReadPreference.primaryPreferred)
  }

  override def findWithPagination(query: JsObject, page: Int, pageSize: Int)(
    implicit ec: ExecutionContext
  ): Future[(Seq[Of], Long)] = collection.flatMap { col =>
    logger.debug(
      s"$tableName.findWithPagination(${Json.prettyPrint(query)}, $page, $pageSize)")
    for {
      count <- col.count(Some(query), None, 0, None, ReadConcern.Majority)
      queryRes <- col
        .find(query, None)
        .sort(Json.obj("_id" -> -1))
        .skip(page * pageSize)
        .batchSize(pageSize)
        .cursor[JsObject](ReadPreference.primaryPreferred)
        .collect[Seq](maxDocs = pageSize, Cursor.FailOnError[Seq[JsObject]]())
        .map(_.map(format.reads).collect {
          case JsSuccess(e, _) => e
        })
    } yield {
      (queryRes, count)
    }
  }

  override def findAll()(implicit ec: ExecutionContext): Future[Seq[Of]] =
    find(Json.obj())

  override def findById(id: String)(
    implicit ec: ExecutionContext): Future[Option[Of]] =
    findOne(Json.obj("_id" -> id))

  override def findById(id: Id)(
    implicit ec: ExecutionContext): Future[Option[Of]] =
    findOne(Json.obj("_id" -> id.value))

  override def deleteById(id: String)(
    implicit ec: ExecutionContext): Future[Int] =
    delete(Json.obj("_id" -> id))

  override def deleteById(id: Id)(
    implicit ec: ExecutionContext): Future[Int] =
    delete(Json.obj("_id" -> id.value))

  override def deleteAll()(implicit ec: ExecutionContext): Future[Int] =
    delete(Json.obj())

  override def exists(id: String)(
    implicit ec: ExecutionContext): Future[Boolean] =
    exists(Json.obj("_id" -> id))

  override def exists(id: Id)(implicit ec: ExecutionContext): Future[Boolean] =
    exists(Json.obj("_id" -> id.value))

  override def findMinByQuery(query: JsObject, field: String)(
    implicit ec: ExecutionContext): Future[Option[Long]] =
    collection.flatMap { col =>
      import col.BatchCommands.AggregationFramework
      import AggregationFramework.{Group, Match, MinField}

      col
        .aggregatorContext[JsObject](
          firstOperator = Match(query),
          otherOperators =
            List(Group(JsString("$clientId"))("min" -> MinField(field))),
          explain = false,
          allowDiskUse = false,
          bypassDocumentValidation = false,
          readConcern = ReadConcern.Majority,
          readPreference = ReadPreference.primaryPreferred,
          writeConcern = WriteConcern.Default,
          batchSize = None,
          cursorOptions = CursorOptions.empty,
          maxTime = None,
          hint = None,
          comment = None,
          collation = None
        )
        .prepared
        .cursor
        .collect[List](1, Cursor.FailOnError[List[JsObject]]())
        .map(agg => agg.headOption.map(v => (v \ "min").as[Long]))
    }

  override def findMaxByQuery(query: JsObject, field: String)(
    implicit ec: ExecutionContext): Future[Option[Long]] =
    collection.flatMap { col =>
      import col.BatchCommands.AggregationFramework
      import AggregationFramework.{Group, Match, MaxField}

      col
        .aggregatorContext[JsObject](
          firstOperator = Match(query),
          otherOperators =
            List(Group(JsString("$clientId"))("max" -> MaxField(field))),
          explain = false,
          allowDiskUse = false,
          bypassDocumentValidation = false,
          readConcern = ReadConcern.Majority,
          readPreference = ReadPreference.primaryPreferred,
          writeConcern = WriteConcern.Default,
          batchSize = None,
          cursorOptions = CursorOptions.empty,
          maxTime = None,
          hint = None,
          comment = None,
          collation = None
        )
        .prepared
        .cursor
        .collect[List](1, Cursor.FailOnError[List[JsObject]]())
        .map(agg => agg.headOption.map(v => (v \ "max").as[Long]))
    }
}

abstract class PostgresTenantAwareRepo[Of, Id <: ValueType](db: PostgresDatabase,
                                                          tenant: TenantId)
  extends Repo[Of, Id] {

  val logger: Logger = Logger(s"PostgresTenantAwareRepo")

  implicit val jsObjectFormat: OFormat[JsObject] = new OFormat[JsObject] {
    override def reads(json: JsValue): JsResult[JsObject] =
      json.validate[JsObject](Reads.JsObjectReads)

    override def writes(o: JsObject): JsObject = o
  }

  val jsObjectWrites: OWrites[JsObject] = (o: JsObject) => o

  def tableName: String

  def collection(implicit ec: ExecutionContext): Future[JSONCollection] =
    db.database.map(_.collection(tableName))

  override def deleteByIdLogically(id: String)(
    implicit ec: ExecutionContext): Future[WriteResult] = {
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update.one(
        q =
          Json.obj("_deleted" -> false, "_id" -> id, "_tenant" -> tenant.value),
        u = Json.obj("$set" -> Json.obj("_deleted" -> true)),
        upsert = false,
        multi = false
      )
    }
  }

  override def deleteByIdLogically(id: Id)(
    implicit ec: ExecutionContext): Future[WriteResult] = {
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update.one(
        q = Json.obj("_deleted" -> false,
          "_id" -> id.value,
          "_tenant" -> tenant.value),
        u = Json.obj("$set" -> Json.obj("_deleted" -> true)),
        upsert = false,
        multi = false
      )
    }
  }

  override def deleteLogically(query: JsObject)(
    implicit ec: ExecutionContext): Future[Boolean] = {
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update
        .element(
          q = query ++ Json.obj("_deleted" -> false, "_tenant" -> tenant.value),
          u = Json.obj("$set" -> Json.obj("_deleted" -> true)),
          upsert = false,
          multi = true)
        .flatMap { element =>
          update.many(List(element)).map(_.ok)
        }
    }
  }

  override def deleteAllLogically()(
    implicit ec: ExecutionContext): Future[Boolean] = {
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update
        .element(q = Json.obj("_deleted" -> false, "_tenant" -> tenant.value),
          u = Json.obj("$set" -> Json.obj("_deleted" -> true)),
          upsert = false,
          multi = true)
        .flatMap { element =>
          update.many(List(element)).map(_.ok)
        }
    }
  }

  override def find(
                     query: JsObject,
                     sort: Option[JsObject] = None,
                     maxDocs: Int = -1)(implicit ec: ExecutionContext): Future[Seq[Of]] =
    collection.flatMap { col =>
      logger.debug(s"$tableName.find(${Json.prettyPrint(
        query ++ Json.obj("_tenant" -> tenant.value))})")
      sort match {
        case None =>
          col
            .find(query ++ Json.obj("_tenant" -> tenant.value), None)
            .cursor[JsObject](ReadPreference.primaryPreferred)
            .collect[Seq](maxDocs = maxDocs,
              Cursor.FailOnError[Seq[JsObject]]())
            .map(
              _.map(format.reads)
                .map {
                  case j @ JsSuccess(_, _) => j
                  case j @ JsError(_) =>
                    println(s"error: $j")
                    j
                }
                .collect {
                  case JsSuccess(e, _) => e
                }
            )
        case Some(s) =>
          col
            .find(query ++ Json.obj("_tenant" -> tenant.value), None)
            .sort(s)
            .cursor[JsObject](ReadPreference.primaryPreferred)
            .collect[Seq](maxDocs = maxDocs,
              Cursor.FailOnError[Seq[JsObject]]())
            .map(
              _.map(format.reads)
                .map {
                  case j @ JsSuccess(_, _) => j
                  case j @ JsError(_) =>
                    println(s"error: $j")
                    j
                }
                .collect {
                  case JsSuccess(e, _) => e
                }
            )
      }
    }

  def findAllRaw()(implicit ec: ExecutionContext): Future[Seq[JsValue]] =
    collection.flatMap { col =>
      logger.debug(s"$tableName.findAllRaw(${Json.prettyPrint(
        Json.obj("_tenant" -> tenant.value))})")
      col
        .find(Json.obj("_tenant" -> tenant.value), None)
        .cursor[JsObject](ReadPreference.primaryPreferred)
        .collect[Seq](maxDocs = -1, Cursor.FailOnError[Seq[JsObject]]())
    }

  def streamAll()(implicit ec: ExecutionContext): Source[Of, NotUsed] =
    Source
      .future(collection.flatMap { col =>
        logger.debug(s"$tableName.streamAll(${Json.prettyPrint(
          Json.obj("_tenant" -> tenant.value))})")
        col
          .find(Json.obj("_tenant" -> tenant.value), None)
          .cursor[JsObject](ReadPreference.primaryPreferred)
          .collect[Seq](maxDocs = -1, Cursor.FailOnError[Seq[JsObject]]())
          .map { docs =>
            docs
              .map(format.reads)
              .map {
                case j @ JsSuccess(_, _) => j
                case j @ JsError(_) =>
                  println(s"error: $j")
                  j
              }
              .collect {
                case JsSuccess(e, _) => e
              }
          }
      })
      .flatMapConcat(seq => Source(seq.toList))

  def streamAllRaw()(implicit ec: ExecutionContext): Source[JsValue, NotUsed] =
    Source
      .future(collection.flatMap { col =>
        logger.debug(s"$tableName.streamAllRaw(${Json.prettyPrint(
          Json.obj("_tenant" -> tenant.value))})")
        col
          .find(Json.obj("_tenant" -> tenant.value), None)
          .cursor[JsObject](ReadPreference.primaryPreferred)
          .collect[Seq](maxDocs = -1, Cursor.FailOnError[Seq[JsObject]]())
      })
      .flatMapConcat(seq => Source(seq.toList))

  override def findOne(query: JsObject)(
    implicit ec: ExecutionContext): Future[Option[Of]] = collection.flatMap {
    col =>
      logger.debug(s"$tableName.findOne(${Json.prettyPrint(
        query ++ Json.obj("_tenant" -> tenant.value))})")
      col
        .find(query ++ Json.obj("_tenant" -> tenant.value), None)
        .one[JsObject](ReadPreference.primaryPreferred)
        .map(_.map(format.reads).collect {
          case JsSuccess(e, _) => e
        })
        .recover(e => {
          logger.error("findOneError", e)
          None
        })
  }

  override def delete(query: JsObject)(
    implicit ec: ExecutionContext): Future[Int] = collection.flatMap {
    col =>
      logger.debug(s"$tableName.delete(${Json.prettyPrint(
        query ++ Json.obj("_tenant" -> tenant.value))})")
      col
        .delete(ordered = true)
        .one(query ++ Json.obj("_tenant" -> tenant.value))
  }

  override def save(value: Of)(
    implicit ec: ExecutionContext): Future[Boolean] = {
    val payload = format.writes(value).as[JsObject]
    save(Json.obj("_id" -> extractId(value)), payload)
  }

  override def save(query: JsObject, value: JsObject)(
    implicit ec: ExecutionContext): Future[Boolean] =
    collection.flatMap { col =>
      logger.debug(
        s"$tableName.upsert(${Json.prettyPrint(query)}, ${Json.prettyPrint(value)})"
      )
      col
        .findAndUpdate(
          selector = query,
          update = value,
          fetchNewObject = false,
          upsert = true,
          sort = None,
          fields = None,
          bypassDocumentValidation = false,
          writeConcern = WriteConcern.Default,
          maxTime = None,
          collation = None,
          arrayFilters = Seq.empty
        )
        .map(_.lastError.isDefined)
    }

  override def insertMany(values: Seq[Of])(
    implicit ec: ExecutionContext): Future[Long] =
    collection.flatMap { col =>
      val payloads = values.map(v =>
        format.writes(v).as[JsObject] ++ Json.obj("_tenant" -> tenant.value))
      col
        .insert(true)
        .many(payloads)
        .map(_.n)
    }

  override def updateMany(query: JsObject, value: JsObject)(
    implicit ec: ExecutionContext): Future[Long] =
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update
        .element(q = query,
          u = Json.obj("$set" -> value),
          upsert = false,
          multi = true)
        .flatMap { element =>
          update.many(List(element)).map(_.nModified)
        }
    }

  override def updateManyByQuery(query: JsObject, queryUpdate: JsObject)(
    implicit ec: ExecutionContext): Future[Long] =
    collection.flatMap { col =>
      val update = col.update(ordered = true)
      update
        .element(q = query, u = queryUpdate, upsert = false, multi = true)
        .flatMap { element =>
          update.many(List(element)).map(_.nModified)
        }
    }

  override def exists(query: JsObject)(
    implicit ec: ExecutionContext): Future[Boolean] = collection.flatMap {
    col =>
      logger.debug(s"$tableName.exists(${Json.prettyPrint(
        query ++ Json.obj("_tenant" -> tenant.value))})")
      col
        .find(query ++ Json.obj("_tenant" -> tenant.value), None)
        .one[JsObject](ReadPreference.primaryPreferred)
        .map(_.isDefined)
  }

  override def findMinByQuery(query: JsObject, field: String)(
    implicit ec: ExecutionContext): Future[Option[Long]] =
    collection.flatMap { col =>
      import col.BatchCommands.AggregationFramework
      import AggregationFramework.{Group, Match, MinField}

      col
        .aggregatorContext[JsObject](
          firstOperator = Match(query),
          otherOperators =
            List(Group(JsString("$clientId"))("min" -> MinField(field))),
          explain = false,
          allowDiskUse = false,
          bypassDocumentValidation = false,
          readConcern = ReadConcern.Majority,
          readPreference = ReadPreference.primaryPreferred,
          writeConcern = WriteConcern.Default,
          batchSize = None,
          cursorOptions = CursorOptions.empty,
          maxTime = None,
          hint = None,
          comment = None,
          collation = None
        )
        .prepared
        .cursor
        .collect[List](1, Cursor.FailOnError[List[JsObject]]())
        .map(agg => agg.headOption.map(v => (v \ "min").as[Long]))
    }

  override def findMaxByQuery(query: JsObject, field: String)(
    implicit ec: ExecutionContext): Future[Option[Long]] =
    collection.flatMap { col =>
      import col.BatchCommands.AggregationFramework
      import AggregationFramework.{Group, Match, MaxField}

      col
        .aggregatorContext[JsObject](
          firstOperator = Match(query),
          otherOperators =
            List(Group(JsString("$clientId"))("max" -> MaxField(field))),
          explain = false,
          allowDiskUse = false,
          bypassDocumentValidation = false,
          readConcern = ReadConcern.Majority,
          readPreference = ReadPreference.primaryPreferred,
          writeConcern = WriteConcern.Default,
          batchSize = None,
          cursorOptions = CursorOptions.empty,
          maxTime = None,
          hint = None,
          comment = None,
          collation = None
        )
        .prepared
        .cursor
        .collect[List](1, Cursor.FailOnError[List[JsObject]]())
        .map(agg => agg.headOption.map(v => (v \ "max").as(json.LongFormat)))
    }

  override def count(query: JsObject)(
    implicit ec: ExecutionContext): Future[Long] =
    collection.flatMap { col =>
      col.count(Some(query), None, 0, None, ReadConcern.Majority)
    }

  override def count()(implicit ec: ExecutionContext): Future[Long] =
    collection.flatMap { col =>
      logger.debug(s"$tableName.count({})")
      col.count(Some(Json.obj("_tenant" -> tenant.value)),
        None,
        0,
        None,
        ReadConcern.Majority)
    }

  override def findWithProjection(query: JsObject, projection: JsObject)(
    implicit ec: ExecutionContext
  ): Future[Seq[JsObject]] = collection.flatMap { col =>
    logger.debug(
      s"$tableName.find(${Json.prettyPrint(query ++ Json.obj(
        "_tenant" -> tenant.value))}, ${Json.prettyPrint(projection)})"
    )
    col
      .find(query ++ Json.obj("_tenant" -> tenant.value), Some(projection))
      .cursor[JsObject](ReadPreference.primaryPreferred)
      .collect[Seq](maxDocs = -1, Cursor.FailOnError[Seq[JsObject]]())
  }

  override def findOneWithProjection(query: JsObject, projection: JsObject)(
    implicit ec: ExecutionContext
  ): Future[Option[JsObject]] = collection.flatMap { col =>
    logger.debug(
      s"$tableName.findOne(${Json.prettyPrint(query ++ Json.obj(
        "_tenant" -> tenant.value))}, ${Json.prettyPrint(projection)})"
    )
    col
      .find(query ++ Json.obj("_tenant" -> tenant.value), Some(projection))
      .one[JsObject](ReadPreference.primaryPreferred)
  }

  override def findWithPagination(query: JsObject, page: Int, pageSize: Int)(
    implicit ec: ExecutionContext
  ): Future[(Seq[Of], Long)] = collection.flatMap { col =>
    logger.debug(
      s"$tableName.findWithPagination(${Json.prettyPrint(query)}, $page, $pageSize)")
    for {
      count <- col.count(Some(query), None, 0, None, ReadConcern.Majority)
      queryRes <- col
        .find(query, None)
        .sort(Json.obj("_id" -> -1))
        .batchSize(pageSize)
        .skip(page * pageSize)
        .cursor[JsObject](ReadPreference.primaryPreferred)
        .collect[Seq](maxDocs = pageSize, Cursor.FailOnError[Seq[JsObject]]())
        .map(_.map(format.reads).collect {
          case JsSuccess(e, _) => e
        })
    } yield {
      (queryRes, count)
    }
  }

  override def findAll()(implicit ec: ExecutionContext): Future[Seq[Of]] =
    find(Json.obj())

  override def findById(id: String)(
    implicit ec: ExecutionContext): Future[Option[Of]] =
    findOne(Json.obj("_id" -> id))

  override def findById(id: Id)(
    implicit ec: ExecutionContext): Future[Option[Of]] =
    findOne(Json.obj("_id" -> id.value))

  override def deleteById(id: String)(
    implicit ec: ExecutionContext): Future[Int] =
    delete(Json.obj("_id" -> id))

  override def deleteById(id: Id)(
    implicit ec: ExecutionContext): Future[Int] =
    delete(Json.obj("_id" -> id.value))

  override def deleteAll()(implicit ec: ExecutionContext): Future[Int] =
    delete(Json.obj())

  override def exists(id: String)(
    implicit ec: ExecutionContext): Future[Boolean] =
    exists(Json.obj("_id" -> id))

  override def exists(id: Id)(implicit ec: ExecutionContext): Future[Boolean] =
    exists(Json.obj("_id" -> id.value))
}

