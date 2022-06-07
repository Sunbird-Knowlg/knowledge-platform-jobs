package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"eid":"BE_QR_IMAGE_GENERATOR","processId":"e101e2cf-d592-4095-89fe-38acc0b8b276","objectId":"do_2132430402739240961409","dialcodes":[{"data":"https://preprod.ntp.net.in/dial/N3X6Y3","text":"N3X6Y3","id":"2_N3X6Y3"},{"data":"https://preprod.ntp.net.in/dial/U3J1J9","text":"U3J1J9","id":"0_U3J1J9"},{"data":"https://preprod.ntp.net.in/dial/J9Z5R4","text":"J9Z5R4","id":"1_J9Z5R4"}],"storage":{"container":"dial","path":"01269878797503692810/","fileName":"do_2132430402739240961409_english_class_10_english_1616582317028"},"config":{"errorCorrectionLevel":"H","pixelsPerBlock":2,"qrCodeMargin":3,"textFontName":"Verdana","textFontSize":11,"textCharacterSpacing":0.1,"imageFormat":"png","colourModel":"Grayscale","imageBorderSize":1}}
      |""".stripMargin

  val EVENT_2: String =
    """
      |{"eid":"BE_QR_IMAGE_GENERATOR","processId":"d36c130b-d5cb-4bec-9224-734d1d89bb00","objectId":"do_2132491902071767041339","dialcodes":[{"data":"https://preprod.ntp.net.in/dial/V2B5A2","text":"V2B5A2","id":"1_V2B5A2","location":"http://location1.com"},{"data":"https://preprod.ntp.net.in/dial/F6J3E7","text":"F6J3E7","id":"0_F6J3E7","location":"http://location2.com"}],"storage":{"container":"dial","path":"01272777697873100812/","fileName":"do_2132491902071767041339_sanskrit_class_12_home_science_1617332855179"},"config":{"errorCorrectionLevel":"H","pixelsPerBlock":2,"qrCodeMargin":3,"textFontName":"Verdana","textFontSize":11,"textCharacterSpacing":0.1,"imageFormat":"png","colourModel":"Grayscale","imageBorderSize":1}}
      |""".stripMargin

  // storageFileName length too long
  val EVENT_3: String =
    """
      |{"eid":"BE_QR_IMAGE_GENERATOR","processId":"d36c130b-d5cb-4bec-9224-734d1d89bb00","objectId":"do_2132491902071767041339","dialcodes":[{"data":"https://preprod.ntp.net.in/dial/V2B5A2","text":"V2B5A2","id":"1_V2B5A2","location":"http://location1.com"},{"data":"https://preprod.ntp.net.in/dial/F6J3E7","text":"F6J3E7","id":"0_F6J3E7","location":"http://location2.com"}],"storage":{"container":"dial","path":"01272777697873100812/","fileName":"do_2132491902071767041339_kannada_english_marathi_hindi_sanskrit_telugu_gujarati_odia_urdu_bengali_tamil_punjabi_malayalam_cpd_class_10_class_11_class_1_class_12_class_2_class_3_class_4_class_5_class_6_class_7_class_8_class_9_others_cbse_training_mathematics_cpd_social_science_information_and_communication_technology_urdu_ict_political_science_civics_geography_heritage_crafts_biology_economics_health_and_physical_education_sociology_chemistry_science_home_science_informatics_practices_graphic_design_business_studies_creative_writing_and_translation_physics_education_computer_science_sanskrit_workbook_english_workbook_statistics_sanskrit_accountancy_english_political_science_hindi_graphics_design_psychology_environmental_studies_history_fine_arts_1617332855179"},"config":{"errorCorrectionLevel":"H","pixelsPerBlock":2,"qrCodeMargin":3,"textFontName":"Verdana","textFontSize":11,"textCharacterSpacing":0.1,"imageFormat":"png","colourModel":"Grayscale","imageBorderSize":1}}
      |""".stripMargin
}