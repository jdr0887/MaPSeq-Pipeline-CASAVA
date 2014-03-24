package edu.unc.mapseq.ws.casava;

import javax.activation.DataHandler;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import javax.jws.soap.SOAPBinding.Style;
import javax.jws.soap.SOAPBinding.Use;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.ws.BindingType;
import javax.xml.ws.soap.MTOM;

import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
@MTOM(enabled = true, threshold = 0)
@BindingType(value = javax.xml.ws.soap.SOAPBinding.SOAP11HTTP_MTOM_BINDING)
@WebService(targetNamespace = "http://casava.ws.mapseq.unc.edu", serviceName = "CASAVAService", portName = "CASAVAPort")
@SOAPBinding(style = Style.DOCUMENT, use = Use.LITERAL, parameterStyle = SOAPBinding.ParameterStyle.WRAPPED)
@Path("/CASAVAService/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface CASAVAService {

    @WebMethod
    public Long uploadSampleSheet(@WebParam(name = "data") DataHandler data,
            @WebParam(name = "flowcellName") String flowcellName);

    @GET
    @Path("/assertDirectoryExists/{studyName}/{flowcell}")
    @WebMethod
    public Boolean assertDirectoryExists(@PathParam("studyName") @WebParam(name = "studyName") String studyName,
            @PathParam("flowcell") @WebParam(name = "flowcell") String flowcell);

}
