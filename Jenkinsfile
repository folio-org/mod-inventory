
buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'yes'
  doKubeDeploy = true
  publishPreview = false
  buildNode = 'jenkins-agent-java17'

  doDocker = {
    buildJavaDocker {
      publishPreview = false
      overrideConfig  = 'no'
      publishMaster = 'yes'
    }
  }

}
