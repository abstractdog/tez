package org.apache.tez.common.web;

import com.google.common.io.ByteStreams;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.tez.dag.api.TezConfiguration;
import org.eclipse.jetty.servlet.DefaultServlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;

public class ProfileOutputServlet extends DefaultServlet {
    public static final String fileQueryParam = "file";
    public ProfileOutputServlet() {

    }
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String queriedFile = request.getParameter(fileQueryParam);
        if (queriedFile == null) {
            response.setContentType(MimeType.TEXT);
            PrintWriter out = response.getWriter();
            out.println("Run the profiler to be able to receive its output");
            out.close();
            return;
        }
        String fileLocation = ProfileServlet.OUTPUT_DIR + '/' + queriedFile;
        File outputFile = new File(fileLocation);
        if (!outputFile.exists()){
            response.setContentType(MimeType.TEXT);
            PrintWriter out = response.getWriter();
            out.println("Requested file does not exist");
            out.close();
            return;
        }
        if (outputFile.length() < 100) {
            response.setContentType(MimeType.TEXT);
            PrintWriter out = response.getWriter();
            out.println("This page auto-refresh every 2 seconds until output file is ready..");
            out.println("Searching in " + fileLocation);
            response.setHeader("Refresh", "2; URL=" + request.getRequestURI() + '?' + fileQueryParam + '=' + queriedFile);
            out.close();
            return;
        }
        response.setContentType(MimeType.HTML);
        response.getOutputStream().write(new FileInputStream(outputFile).readAllBytes());
        response.getOutputStream().flush();
    }
}
