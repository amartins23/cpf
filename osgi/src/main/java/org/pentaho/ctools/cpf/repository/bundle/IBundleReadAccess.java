package org.pentaho.ctools.cpf.repository.bundle;

import org.osgi.framework.Bundle;
import pt.webdetails.cpf.repository.api.IReadAccess;

public interface IBundleReadAccess extends IReadAccess {
  boolean isUserContent();
}
