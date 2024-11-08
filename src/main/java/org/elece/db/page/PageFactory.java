package org.elece.db.page;

import org.elece.exception.DbException;
import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;

public interface PageFactory {
    Page getPage(PageTitle pageTitle) throws DbException, InterruptedTaskException, StorageException,
                                             FileChannelException;
}
