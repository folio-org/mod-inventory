
buildMvn {
  publishModDescriptor = 'yes'
  publishAPI = 'yes'
  mvnDeploy = 'yes'
  runLintRamlCop = 'yes'
  doKubeDeploy = true
  publishPreview = 'yes'

  doDocker = {
    buildJavaDocker {
      overrideConfig  = 'no'
      publishMaster = 'yes'
      publishPreview = 'yes'
    }
  }

}
