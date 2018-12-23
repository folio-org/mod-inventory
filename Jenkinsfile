@Library ('folio_jenkins_shared_libs@folio-1481-lint-raml-two-jobs') _

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
