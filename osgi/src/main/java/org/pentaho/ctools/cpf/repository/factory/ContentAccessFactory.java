/*!
 * Copyright 2018 Webdetails, a Hitachi Vantara company. All rights reserved.
 *
 * This software was developed by Webdetails and is provided under the terms
 * of the Mozilla Public License, Version 2.0, or any later version. You may not use
 * this file except in compliance with the license. If you need a copy of the license,
 * please go to http://mozilla.org/MPL/2.0/. The Initial Developer is Webdetails.
 *
 * Software distributed under the Mozilla Public License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. Please refer to
 * the license for the specific language governing your rights and limitations.
 */
package org.pentaho.ctools.cpf.repository.factory;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.ctools.cpf.repository.bundle.ReadAccessProxy;
import org.pentaho.ctools.cpf.repository.bundle.UserContentAccess;
import org.pentaho.ctools.cpf.repository.utils.FileSystemRWAccess;
import org.pentaho.ctools.cpf.repository.utils.OverlayRWAccess;
import org.pentaho.ctools.cpf.repository.utils.OverlayUserContentAccess;
import pt.webdetails.cpf.api.IContentAccessFactoryExtended;
import pt.webdetails.cpf.api.IUserContentAccessExtended;
import pt.webdetails.cpf.repository.api.IReadAccess;
import pt.webdetails.cpf.repository.api.IRWAccess;


/**
 * The {@code ContentAccessFactory} class creates repository access providers for basic plugin needs.
 * These access providers are instances of {@code ReadAccessProxy} that contain a reference to an internal dynamic list
 * of available {@code IReadAccess} services that allow access to the available resources.
 *
 * Additionally, user content {@code IUserContentAccess} can also use a single dynamic instance of a {@code IRWAccess}
 * service to provide write operations.
 *
 * Note: To facilitate operations by CDE Editor, a dummy instance is returned from {@code getPluginSystemWriter} and
 * {@code getOtherPluginSystemWriter} that fakes write operations and forwards read operations to an instance of
 * {@code ReadAccessProxy}.
 *
 * Note: PluginRepository write access is currently not supported.
 *
 * @see IContentAccessFactoryExtended
 * @see IUserContentAccessExtended
 * @see IReadAccess
 * @see IRWAccess
 */
public final class ContentAccessFactory implements IContentAccessFactoryExtended {
  private static final Log logger = LogFactory.getLog( ContentAccessFactory.class );
  private static final String SERVICE_PROPERTY_PLUGIN_ID = "pluginId";
  private static final String PLUGIN_REPOS_NAMESPACE = "repos";
  private static final String PLUGIN_SYSTEM_NAMESPACE = "system";
  private Map<String, List<IReadAccess>> pluginReadAccessMap = new HashMap<>();
  private List<IReadAccess> userContentReadAccesses = new ArrayList<>();
  private IUserContentAccessExtended userContentAccess = null;
  private final String volumePath;
  private final String parentPluginId;
  private FileSystem storageFilesystem = FileSystems.getDefault();

  /**
   * Add new read-only content to the plugin system namespace.
   * @param pluginId identifier of the plugin
   * @param readAccess instance of read access to the plugin resources
   */
  public void addPluginSystemAccess( String pluginId, IReadAccess readAccess ) {
    List<IReadAccess> pluginList = getPluginSystemAccessList( pluginId );
    pluginList.add( readAccess );
  }

  private List<IReadAccess> getPluginSystemAccessList( String pluginId ) {
    List<IReadAccess> pluginList;

    if ( !this.pluginReadAccessMap.containsKey( pluginId ) ) {
      pluginList = new ArrayList<>();
      this.pluginReadAccessMap.put( pluginId, pluginList );
    } else {
      pluginList = this.pluginReadAccessMap.get( pluginId );
    }

    return pluginList;
  }

  /**
   * Remove read-only content to the plugin system namespace.
   * @param pluginId identifier of the plugin
   * @param readAccess instance of read access to the plugin resources
   */
  public void removePluginSystemAccess( String pluginId, IReadAccess readAccess ) {
    List<IReadAccess> pluginList = this.pluginReadAccessMap.get( pluginId );
    if ( pluginList != null ) {
      pluginList.remove( readAccess );
    }
  }

  public void addReadAccess( IReadAccess readAccess, Map serviceProperties ) {
    if ( !serviceProperties.isEmpty() ) {
      Object id = serviceProperties.get( SERVICE_PROPERTY_PLUGIN_ID );
      if ( id != null && id instanceof String ) {
        addPluginSystemAccess( (String) id, readAccess );
      }
    }
  }

  public void removeReadAccess( IReadAccess readAccess, Map serviceProperties ) {
    if ( !serviceProperties.isEmpty() ) {
      Object id = serviceProperties.get( SERVICE_PROPERTY_PLUGIN_ID );
      if ( id != null && id instanceof String ) {
        removePluginSystemAccess( (String) id, readAccess );
      }
    }
  }

  public void setUserContentAccess( IUserContentAccessExtended userContentAccess ) {
    this.userContentAccess = userContentAccess;
  }

  public void removeUserContentAccess( IUserContentAccessExtended userContentAccess ) {
    this.userContentAccess = null;
  }

  public ContentAccessFactory( String parentPluginId ) {
    this.volumePath = System.getProperty( "java.io.tmpdir" );
    this.parentPluginId = parentPluginId;
  }

  public ContentAccessFactory( String volumePath, String parentPluginId ) {
    this.volumePath = volumePath;
    this.parentPluginId = parentPluginId;
  }

  @Override
  public IUserContentAccessExtended getUserContentAccess( String basePath ) {
    if ( userContentAccess == null ) {
      return new UserContentAccess( new ReadAccessProxy( userContentReadAccesses, basePath ) );
    } else {
      if ( userContentReadAccesses.isEmpty() ) {
        return userContentAccess;
      } else {
        return new OverlayUserContentAccess( basePath, userContentAccess, userContentReadAccesses );
      }
    }
  }

  @Override
  public IReadAccess getPluginRepositoryReader( String basePath ) {
    logger.info( "RO FileSystemOverlay for: " + basePath );
    return getPluginRepositoryOverlay( basePath );
  }

  @Override
  public IRWAccess getPluginRepositoryWriter( String basePath ) {
    logger.info( "RO FileSystemOverlay for: " + basePath );
    return getPluginRepositoryOverlay( basePath );
  }

  @Override
  public IReadAccess getPluginSystemReader( String basePath ) {
    return getOtherPluginSystemReader( parentPluginId, basePath );
  }

  @Override
  public IRWAccess getPluginSystemWriter( String basePath ) {
    return getOtherPluginSystemWriter( parentPluginId, basePath );
  }

  @Override
  public IReadAccess getOtherPluginSystemReader( String pluginId, String basePath ) {
    logger.info( ( pluginId.equals( parentPluginId ) ? "[SELF]" : "[OTHER]" ) + " RO FileSystemOverlay for <" + pluginId + ">: " + basePath );
    return getPluginSystemOverlay( pluginId, basePath );
  }

  @Override
  public IRWAccess getOtherPluginSystemWriter( String pluginId, String basePath ) {
    logger.info( ( pluginId.equals( parentPluginId ) ? "[SELF]" : "[OTHER]" ) + " RO FileSystemOverlay for <" + pluginId + ">: " + basePath );
    return getPluginSystemOverlay( pluginId, basePath );
  }

  private IRWAccess getPluginRepositoryOverlay( String basePath ) {
    // implemented as a filesystem folder on foundry, as it is a storage area common to all users
    String storagePath = createStoragePath( PLUGIN_REPOS_NAMESPACE );
    return new FileSystemRWAccess( FileSystems.getDefault(), storagePath, basePath );
  }

  private IRWAccess getPluginSystemOverlay( String pluginId, String basePath ) {
    // combine read-write via filesystem storage with bundle supplied read-only assets
    String storagePath = createStoragePath( PLUGIN_SYSTEM_NAMESPACE, pluginId );
    IRWAccess fileSystemWriter = new FileSystemRWAccess( storageFilesystem, storagePath, null );
    return new OverlayRWAccess( basePath, fileSystemWriter, getPluginSystemAccessList( pluginId ) );
  }

  private String createStoragePath( String namespace ) {
    return createStoragePath( namespace, null );
  }

  private String createStoragePath( String namespace, String id ) {
    // TODO: validate that basePath does not cross back the namespace boundary
    Path storagePath =  id != null ? storageFilesystem.getPath( volumePath, namespace, id ) : storageFilesystem.getPath( volumePath, namespace );
    File storage = storagePath.toFile();
    if ( storage.exists() && !storage.isDirectory() ) {
      throw new IllegalStateException( "Expected path to be a directory: " + storagePath.toString() );
    }
    if ( !storage.exists() ) {
      storage.mkdirs();
    }
    return storagePath.toString();
  }

  public FileSystem getPluginStorageFilesystem() {
    return storageFilesystem;
  }

  public void setPluginStorageFilesystem( FileSystem storageFilesystem ) {
    this.storageFilesystem = storageFilesystem;
  }
}
