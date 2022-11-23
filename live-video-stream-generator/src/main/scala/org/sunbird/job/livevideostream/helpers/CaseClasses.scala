package org.sunbird.job.livevideostream.helpers

import org.joda.time.DateTime
import scala.collection.immutable.HashMap


case class MediaRequest(id: String, params: Map[String, AnyRef] = new HashMap[String, AnyRef], request: Map[String, AnyRef] = new HashMap[String, AnyRef]);

case class MediaResponse(id: String, ts: String, params: Map[String, Any] = new HashMap[String, AnyRef], responseCode: String, result: Map[String, AnyRef] = new HashMap[String, AnyRef])

object ResponseCode extends Enumeration {
  type Code = Value
  val OK = Value(200)
  val CLIENT_ERROR = Value(400)
  val SERVER_ERROR = Value(500)
  val RESOURCE_NOT_FOUND = Value(404)
}

trait AlgoOutput extends AnyRef

case class JobRequest(client_key: String, request_id: String, job_id: Option[String], status: String, request_data: String, iteration: Int, dt_job_submitted: Option[DateTime] = None, location: Option[String] = None, dt_file_created: Option[DateTime] = None, dt_first_event: Option[DateTime] = None, dt_last_event: Option[DateTime] = None, dt_expiration: Option[DateTime] = None, dt_job_processing: Option[DateTime] = None, dt_job_completed: Option[DateTime] = None, input_events: Option[Int] = None, output_events: Option[Int] = None, file_size: Option[Long] = None, latency: Option[Int] = None, execution_time: Option[Long] = None, err_message: Option[String] = None, stage: Option[String] = None, stage_status: Option[String] = None, job_name: Option[String] = None) extends AlgoOutput

case class JobStage(request_id: String, client_key: String, stage: String, stage_status: String, status: String, err_message: String = "", dt_job_processing: Option[DateTime] = Option(new DateTime()))
case class StreamingStage(request_id: String, client_key: String, job_id: String, stage: String, stage_status: String, status: String, iteration: Int, err_message: String = "")