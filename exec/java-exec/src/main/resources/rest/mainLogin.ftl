<#--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->
<#include "*/generic.ftl">
<#macro page_head>
  <script type="text/javascript" src="/static/js/mainLogin.js"></script>
</#macro>

<#macro page_body>
  <div class="container">
    <#if model?? && model.isFormEnabled()>
      <div class="row justify-content-center ">
        <div class="col-md-4">
          <a href="/login" class="btn btn-primary btn-block"> Login using FORM AUTHENTICATION </a>
        </div>
      </div>
    </#if>
    <#if model?? && model.isSpnegoEnabled()>
      <div class="row justify-content-center mt-2">
        <div class="col-md-4">
          <a href="/spnegoLogin" class="btn btn-primary btn-block"> Login using SPNEGO </a>
        </div>
      </div>
    </#if>
    <#if model?? && model.isOpenIdEnabled()>
      <div class="row justify-content-center mt-2">
        <div class="col-md-4">
          <a id="openid-button" href="/openid" class="btn btn-primary btn-block"> Login using SSO </a>
        </div>
      </div>
    <#else>
      <div class="row justify-content-center mt-2">
        <div class="col-md-4">
          <a id="openid-button" href="/openid" class="btn btn-primary btn-block disabled"> Login using
            SSO </a>
        </div>
      </div>
    </#if>
    <#if model?? && model.getError()??>
      <div class="row justify-content-center mt-3">
        <div class="col-md-4 text-center">
          <span id="error-message" class="text-danger">${model.getError()}</span>
        </div>
      </div>
    </#if>
  </div>
</#macro>
<@page_html/>
