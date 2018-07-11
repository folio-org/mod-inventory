@Library ('folio_jenkins_shared_libs@FOLIO-1027-ci-lint-raml-cop') _

buildMvn {
  publishModDescriptor = 'yes'
  publishAPI = 'yes'
  mvnDeploy = 'yes'
  runLintRamlCop = 'yes'

  doDocker = {
    buildJavaDocker {
      overrideConfig  = 'no'
      publishMaster = 'yes'
    }
  }

}
