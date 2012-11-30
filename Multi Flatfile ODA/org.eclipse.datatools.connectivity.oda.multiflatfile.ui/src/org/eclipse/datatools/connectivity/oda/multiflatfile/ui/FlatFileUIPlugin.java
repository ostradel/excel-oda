/*
 *************************************************************************
 * Copyright (c) 2005, 2006 Actuate Corporation.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *  Actuate Corporation  - initial API and implementation
 *  
 *************************************************************************
 */

package org.eclipse.datatools.connectivity.oda.multiflatfile.ui;

import org.eclipse.core.runtime.Plugin;
import org.osgi.framework.BundleContext;

public class FlatFileUIPlugin extends Plugin
{
    public void start( BundleContext context ) throws Exception
    {
        super.start( context );
        if( isDebugging() )
        {
            // TODO - set up logger based on debug option
        }
    }
}
