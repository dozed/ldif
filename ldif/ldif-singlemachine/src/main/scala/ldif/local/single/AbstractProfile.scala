/* 
 * LDIF
 *
 * Copyright 2011 Freie Universität Berlin, MediaEvent Services GmbH & Co. KG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldif.local.single

import ldif.local.EntityBuilder

/**
 * Created by IntelliJ IDEA.
 * User: andreas
 * Date: 15.06.11
 * Time: 16:27
 * To change this template use File | Settings | File Templates.
 */

abstract class AbstractProfile {
  val threadedDumpLoad: Boolean
  val threadedCreateHTs: Boolean
  val entityBuilder: Class[EntityBuilder]
}

class DefaultProfile extends AbstractProfile {
  val threadedDumpLoad = false
  val threadedCreateHTs = false
  val entityBuilder = classOf[EntityBuilder]
}