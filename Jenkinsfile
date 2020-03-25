
buildMvn {
  publishModDescriptor = 'yes'
  publishAPI = 'yes'
  mvnDeploy = 'yes'
  runLintRamlCop = 'yes'
  doKubeDeploy = true
  publishPreview = true

  doDocker = {
    buildJavaDocker {
      publishPreview = true
      overrideConfig  = 'no'
      publishMaster = 'yes'
    }
  }

}
