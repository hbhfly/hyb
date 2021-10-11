package com.hybris.es.cloud.azure.hotfolder;

import de.hybris.platform.cloud.hotfolder.dataimport.batch.zip.ZipBatchHeader;

import java.io.File;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hybris.platform.util.CSVConstants;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import de.hybris.platform.util.Config;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.file.filters.FileListFilter;
import org.springframework.integration.file.filters.ResettableFileListFilter;
import org.springframework.integration.file.filters.ReversibleFileListFilter;
import org.springframework.integration.file.remote.RemoteFileTemplate;
import org.springframework.integration.file.remote.session.Session;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.file.remote.synchronizer.AbstractInboundFileSynchronizer;
import org.springframework.integration.file.remote.synchronizer.InboundFileSynchronizer;
import org.springframework.messaging.MessagingException;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/*
    Replace default ZipHeaderSetupTask class
    Initially setup the batch header. The header is used throughout the pipeline as a reference and for cleanup.

    This customization allows multi catalog management by fetching catalog name through regex pattern
 */
@Component
public class MultiZipHeaderSetupTask {
        private static final Logger LOG = LoggerFactory.getLogger(MultiZipHeaderSetupTask.class);

        private File unzippedRootDirectory;
        private String headerKeyFile;
        private String headerKeyUnzippedFolder;
        private String vendorPattern;
        private boolean net;

        public ZipBatchHeader execute(final Message<?> message)
        {
            Assert.isTrue(message.getPayload() instanceof Map, "message.payload to be of type Map.class");
            final Map<Object, File> unzippedFiles = (Map) message.getPayload();

            final String fileName = (String) message.getHeaders().get(headerKeyFile);
            Assert.notNull(fileName, "message.headers should contain '" + headerKeyFile + "'");
            final String explodedFolderName = fileName.substring(0, fileName.length() - 4);

            final String unzippedFolderName = message.getHeaders().get(headerKeyUnzippedFolder).toString();
            Assert.notNull(unzippedFolderName, "message.headers should contain '" + headerKeyUnzippedFolder + "'");
            final File unzippedAs = new File(this.unzippedRootDirectory, String.join(File.separator, unzippedFolderName, explodedFolderName));

            final ZipBatchHeader header = new ZipBatchHeader();
            header.setOriginalFileName(fileName);
            header.setFileUnzippedAs(unzippedAs);
            header.setUnzippedFiles(unzippedFiles.values());
            header.setEncoding(CSVConstants.HYBRIS_ENCODING);
            header.setCatalog(extractCatalogID(fileName));
            header.setNet(net);
            return header;
        }

        private String extractCatalogID(String filename) {
            Pattern pattern = Pattern.compile(vendorPattern);
            Matcher matcher = pattern.matcher(filename);
            String catalog = "";
            if (matcher.find())
            {
                String venderId = matcher.group(1);
                catalog = Config.getParameter("cloud.hotfolder." + venderId + ".zip.header.catalog");
                LOG.info("Catalog name extracted [{}] ", catalog);
            } else {
                catalog = Config.getParameter("cloud.hotfolder.default.zip.header.catalog");
                LOG.info("Catalog default name extracted [{}] ", catalog);
            }
            return catalog;
        }

        /**
         * Set the root directory of where the zip was extracted into
         */
        @Required
        public void setUnzippedRootDirectory(final File unzippedRootDirectory)
        {
            this.unzippedRootDirectory = unzippedRootDirectory;
        }

        /**
         * Set the header key to use to get a reference to the original file
         */
        @Required
        public void setHeaderKeyFile(final String headerKeyFile)
        {
            this.headerKeyFile = headerKeyFile;
        }

        /**
         * Set the header key to use to get the name of the folder the file was extracted into
         */
        @Required
        public void setHeaderKeyUnzippedFolder(final String headerKeyUnzippedFolder)
        {
            this.headerKeyUnzippedFolder = headerKeyUnzippedFolder;
        }

        @Required
        public void setVendorPattern(final String vendorPattern)
        {
            this.vendorPattern = vendorPattern;
        }

        @Required
        public void setNet(final boolean net)
        {
            this.net = net;
        }
    }
