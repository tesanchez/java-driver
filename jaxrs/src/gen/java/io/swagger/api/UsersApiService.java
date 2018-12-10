package io.swagger.api;

import io.swagger.api.*;
import io.swagger.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import io.swagger.model.InlineResponse200;
import io.swagger.model.InlineResponse400;

import java.util.List;
import io.swagger.api.NotFoundException;

import java.io.InputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2018-12-10T19:41:44.238Z")
public abstract class UsersApiService {
    public abstract Response usersDelete(SecurityContext securityContext) throws NotFoundException;
    public abstract Response usersGet(SecurityContext securityContext) throws NotFoundException;
}
