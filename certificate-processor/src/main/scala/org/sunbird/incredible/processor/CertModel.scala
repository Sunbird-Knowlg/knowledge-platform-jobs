package org.sunbird.incredible.processor

import org.sunbird.incredible.pojos.ob.{Criteria, Issuer, SignatoryExtension}

case class CertModel(courseName: String, recipientName: String, recipientId: Option[String] = None, recipientEmail: Option[String] = None, recipientPhone: Option[String] = None
                     , certificateName: String, certificateDescription: Option[String] = None, certificateLogo: Option[String] = None, issuedDate: String, issuer: Issuer,
                     validFrom: Option[String] = None, expiry: Option[String] = None, signatoryList: Array[SignatoryExtension], assessedOn: Option[String] = None, identifier: String, criteria: Criteria)
